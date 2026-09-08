//! Identical cycle admission, operation ownership and bounded histograms for both engines.
use crate::{metadata_metrics::Histogram, metadata_workload as workload};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::Instant;

#[derive(Default)]
struct FamilyStats {
    successful: Histogram,
    rejected: Histogram,
}

#[derive(Default)]
pub struct WorkerStats {
    pub operations: u64,
    families: BTreeMap<&'static str, FamilyStats>,
}

impl WorkerStats {
    fn observe(&mut self, result: workload::OperationResult) {
        self.operations += 1;
        for item in result.observations {
            let family = self.families.entry(item.family.as_str()).or_default();
            let histogram = if item.rejected {
                &mut family.rejected
            } else {
                &mut family.successful
            };
            histogram.observe(Duration::from_nanos(item.elapsed_ns));
        }
    }

    fn merge(&mut self, other: Self) {
        self.operations += other.operations;
        for (name, family) in other.families {
            let target = self.families.entry(name).or_default();
            target.successful.merge(&family.successful);
            target.rejected.merge(&family.rejected);
        }
    }

    pub fn report(&self) -> Value {
        let empty = FamilyStats::default();
        workload::Family::ALL
            .iter()
            .map(|kind| {
                let name = kind.as_str();
                let family = self.families.get(name).unwrap_or(&empty);
                (
                    name.to_owned(),
                    json!({"successful":family.successful.report(),
                "expected_rejected":family.rejected.report()}),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }
}

pub async fn load(
    fixture: Arc<workload::Fixture>,
    concurrency: usize,
    stop: Instant,
    deadline: Instant,
) -> Result<WorkerStats, String> {
    let mut jobs = tokio::task::JoinSet::new();
    for worker in 0..concurrency {
        let fixture = fixture.clone();
        jobs.spawn(async move {
            let mut stats = WorkerStats::default();
            let mut sequence = 0;
            // Finish the admitted five-bundle mix cycle; measured wall includes its tail.
            while Instant::now() < stop || sequence % 5 != 0 {
                stats.observe(fixture.operation(worker, sequence, deadline).await?);
                sequence += 1;
                if sequence > 1_000_000 {
                    return Err("worker operation ceiling exceeded".to_owned());
                }
            }
            Ok::<_, String>(stats)
        });
    }
    let mut total = WorkerStats::default();
    let mut failure = None;
    while let Some(result) = jobs.join_next().await {
        match result
            .map_err(|error| error.to_string())
            .and_then(|result| result)
        {
            Ok(stats) => total.merge(stats),
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(total)
}
