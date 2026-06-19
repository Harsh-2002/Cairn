//! In-process request-metrics aggregator (ARCH 26.5). Every completed S3 and management API
//! request is counted into a sharded set of maps with **zero database I/O on the hot path**: a
//! request only takes one shard lock for the few microseconds it costs to bump a `u64`. A
//! background flush periodically [`drain`](RequestMetricsAgg::drain)s the accumulated counts into a
//! batched upsert through the single metadata writer.
//!
//! The sharding mirrors the idiom in `cairn-meta`'s config cache: a fixed power-of-two number of
//! independent `Mutex<HashMap<…>>` shards chosen by key hash, so concurrent recorders for different
//! keys rarely contend on the same lock.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

use cairn_types::meta::RequestMetricRow;

/// Number of lock shards. A power of two keeps the hash-to-shard reduction a single mask.
const SHARDS: usize = 16;

/// Per-shard cap on distinct metric keys (audit #22). The `bucket` dimension is derived from the
/// request path, so a client can spray arbitrarily many distinct (even well-formed) bucket labels;
/// without a cap that grows the in-process map and the flushed `request_metrics` time series
/// without bound. Once a shard is saturated, a *new* key folds into [`OVERFLOW_BUCKET`] so total
/// cardinality stays bounded by `SHARDS * MAX_KEYS_PER_SHARD`. Real deployments key by a bounded
/// op × status × touched-bucket set per flush window, comfortably under this ceiling.
const MAX_KEYS_PER_SHARD: usize = 4096;

/// Sentinel `bucket` label that overflow keys fold into once a shard is saturated. The additive
/// flush upsert coalesces it across shards, so it may legitimately appear in several shard maps.
const OVERFLOW_BUCKET: &str = "__other__";

/// The composite key one count accumulates under: the rollup window, the operation name, the
/// targeted bucket (`""` for non-bucket ops), and the HTTP status class (a `&'static str`, one of
/// `"2xx"`, `"3xx"`, `"4xx"`, `"5xx"`).
type MetricKey = (i64, String, String, &'static str);

/// Map an HTTP status code to its low-cardinality class label.
fn status_class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}

/// Compute the shard index for a key.
fn shard_of(key: &MetricKey) -> usize {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) & (SHARDS - 1)
}

/// The per-key accumulated totals for a rollup window: the request count alongside the byte
/// throughput and latency aggregates (sum + a fixed-width histogram). One [`Cell`] coalesces every
/// request that maps to the same [`MetricKey`].
#[derive(Default, Clone)]
struct Cell {
    count: u64,
    bytes_in: u64,
    bytes_out: u64,
    lat_sum_ms: u64,
    lat_hist: [u64; cairn_types::LATENCY_BUCKETS],
}

/// A sharded, in-process accumulator of per-window request counts. Cheap to `record` into
/// (one shard lock, one hashmap bump) and drained in batches by the background flush loop.
pub struct RequestMetricsAgg {
    shards: Vec<Mutex<HashMap<MetricKey, Cell>>>,
    /// The rollup window granularity in seconds; counts are floored to this window.
    bucket_secs: i64,
}

impl std::fmt::Debug for RequestMetricsAgg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestMetricsAgg")
            .field("shards", &self.shards.len())
            .field("bucket_secs", &self.bucket_secs)
            .finish()
    }
}

impl RequestMetricsAgg {
    /// Construct an aggregator that floors timestamps to a `bucket_secs`-second window. A
    /// `bucket_secs` of `0` is coerced to `1` so the floor division never divides by zero (config
    /// validation already forbids `0` when the subsystem is enabled; this is belt-and-braces).
    #[must_use]
    pub fn new(bucket_secs: u64) -> Self {
        let mut shards = Vec::with_capacity(SHARDS);
        for _ in 0..SHARDS {
            shards.push(Mutex::new(HashMap::new()));
        }
        #[allow(clippy::cast_possible_wrap)]
        let bucket_secs = (bucket_secs.max(1)) as i64;
        Self {
            shards,
            bucket_secs,
        }
    }

    /// Count one completed request. `operation` is the classified op name, `bucket` the targeted
    /// bucket (`""` for non-bucket ops), `status` the HTTP status code, `latency_ms` the request
    /// duration in whole milliseconds, `bytes_in`/`bytes_out` the received/sent byte counts, and
    /// `now_secs` the current epoch seconds. The lock is held only for the hashmap bump.
    // Each parameter is an independent dimension of the metric sample; bundling them into a struct
    // at this hot call site buys no clarity, so the eight-argument shape is deliberate.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        operation: &str,
        bucket: &str,
        status: u16,
        latency_ms: u64,
        bytes_in: u64,
        bytes_out: u64,
        now_secs: i64,
    ) {
        let ts_bucket = (now_secs / self.bucket_secs) * self.bucket_secs;
        let key: MetricKey = (
            ts_bucket,
            operation.to_owned(),
            bucket.to_owned(),
            status_class(status),
        );
        let shard = &self.shards[shard_of(&key)];
        let mut guard = shard.lock().unwrap();
        // Cardinality guard (audit #22): once the shard is full, a new (attacker-influenceable)
        // bucket label must not grow the map — fold it into the sentinel instead. An already-known
        // key keeps accumulating in place.
        let cell = if guard.len() >= MAX_KEYS_PER_SHARD && !guard.contains_key(&key) {
            let overflow: MetricKey = (
                ts_bucket,
                operation.to_owned(),
                OVERFLOW_BUCKET.to_owned(),
                status_class(status),
            );
            guard.entry(overflow).or_default()
        } else {
            guard.entry(key).or_default()
        };
        cell.count += 1;
        cell.bytes_in += bytes_in;
        cell.bytes_out += bytes_out;
        cell.lat_sum_ms += latency_ms;
        cell.lat_hist[cairn_types::latency_bucket_index(latency_ms)] += 1;
    }

    /// Atomically swap every shard's map out and flatten the accumulated counts into rows for a
    /// batched upsert. Returns an empty vector when no traffic has been recorded since the last
    /// drain.
    #[must_use]
    pub fn drain(&self) -> Vec<RequestMetricRow> {
        let mut rows = Vec::new();
        for shard in &self.shards {
            let taken = {
                let mut guard = shard.lock().unwrap();
                std::mem::take(&mut *guard)
            };
            for ((ts_bucket, operation, bucket, status_class), cell) in taken {
                rows.push(RequestMetricRow {
                    ts_bucket,
                    operation,
                    bucket,
                    status_class: status_class.to_owned(),
                    count: cell.count,
                    bytes_in: cell.bytes_in,
                    bytes_out: cell.bytes_out,
                    lat_sum_ms: cell.lat_sum_ms,
                    lat_hist: cell.lat_hist,
                });
            }
        }
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_classes_bucket_correctly() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(204), "2xx");
        assert_eq!(status_class(301), "3xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(500), "5xx");
        assert_eq!(status_class(0), "5xx");
    }

    #[test]
    fn empty_drain_when_no_traffic() {
        let agg = RequestMetricsAgg::new(60);
        assert!(agg.drain().is_empty());
    }

    #[test]
    fn floors_to_window_and_coalesces() {
        let agg = RequestMetricsAgg::new(60);
        // Three requests in the same minute window for the same key coalesce into one row.
        // Latency samples 3/30/300 ms land in distinct histogram buckets (indexes 0/2/3).
        agg.record("GetObject", "b", 200, 3, 10, 100, 125);
        agg.record("GetObject", "b", 204, 30, 20, 200, 150);
        agg.record("GetObject", "b", 299, 300, 30, 300, 179);
        let rows = agg.drain();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(
            row.ts_bucket, 120,
            "125/150/179 all floor to the 120s window"
        );
        assert_eq!(row.operation, "GetObject");
        assert_eq!(row.bucket, "b");
        assert_eq!(row.status_class, "2xx");
        assert_eq!(row.count, 3);
        // Bytes and latency accumulate across the coalesced requests.
        assert_eq!(row.bytes_in, 60, "10 + 20 + 30");
        assert_eq!(row.bytes_out, 600, "100 + 200 + 300");
        assert_eq!(row.lat_sum_ms, 333, "3 + 30 + 300");
        assert_eq!(
            row.lat_hist[cairn_types::latency_bucket_index(3)],
            1,
            "the 3 ms sample fell in its histogram bucket"
        );
        assert_eq!(
            row.lat_hist[cairn_types::latency_bucket_index(30)],
            1,
            "the 30 ms sample fell in its histogram bucket"
        );
        assert_eq!(
            row.lat_hist[cairn_types::latency_bucket_index(300)],
            1,
            "the 300 ms sample fell in its histogram bucket"
        );
        assert_eq!(
            row.lat_hist.iter().sum::<u64>(),
            row.count,
            "every request contributes exactly one histogram entry"
        );
        // Draining clears the accumulator.
        assert!(agg.drain().is_empty());
    }

    #[test]
    fn bucket_cardinality_is_capped() {
        // Audit #22: spraying more distinct bucket labels than the cap must not grow the rollup
        // without bound — excess folds into the sentinel, and no request is lost.
        let agg = RequestMetricsAgg::new(60);
        let n = SHARDS * MAX_KEYS_PER_SHARD + 5_000;
        for i in 0..n {
            agg.record("GetObject", &format!("bucket-{i}"), 200, 1, 0, 0, 0);
        }
        let rows = agg.drain();
        // Every recorded request is still counted (real key or sentinel) — nothing dropped.
        let total: u64 = rows.iter().map(|r| r.count).sum();
        assert_eq!(
            total as usize, n,
            "every recorded request is counted somewhere"
        );
        // Distinct keys are bounded by the per-shard cap (+1 sentinel per shard), far below the
        // `n` distinct buckets we sprayed.
        assert!(
            rows.len() <= SHARDS * (MAX_KEYS_PER_SHARD + 1),
            "cardinality capped, got {} rows",
            rows.len()
        );
        assert!(rows.len() < n, "folding actually happened");
        // The excess landed in the overflow sentinel.
        let overflow: u64 = rows
            .iter()
            .filter(|r| r.bucket == OVERFLOW_BUCKET)
            .map(|r| r.count)
            .sum();
        assert!(overflow > 0, "overflow keys folded into the sentinel");
    }

    #[test]
    fn distinct_keys_stay_separate() {
        let agg = RequestMetricsAgg::new(60);
        agg.record("GetObject", "b", 200, 1, 0, 0, 0); // 2xx
        agg.record("GetObject", "b", 404, 1, 0, 0, 0); // different status class
        agg.record("PutObject", "b", 200, 1, 0, 0, 0); // different op
        agg.record("GetObject", "c", 200, 1, 0, 0, 0); // different bucket
        agg.record("GetObject", "b", 200, 1, 0, 0, 60); // different window
        let rows = agg.drain();
        assert_eq!(rows.len(), 5, "five distinct keys, none coalesced");
        assert!(rows.iter().all(|r| r.count == 1));
    }
}
