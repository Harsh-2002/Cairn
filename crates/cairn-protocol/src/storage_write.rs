//! Runtime-neutral ownership of one admitted physical write (ARCH 8).

use cairn_types::error::Error;
use cairn_types::meta::Mutation;
use cairn_types::storage::io::{StorageIoLease, StorageIoWatch};
use cairn_types::storage::{
    PlannedStorageWrite, StorageAdmission, StorageCreationPermit, StorageToken, StorageWritePlan,
    StorageWriteTarget,
};
use cairn_types::{BlobStore, BucketName};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// One runtime-owned recovery slot. Clones retain the same admission until all associated
/// request, queue and actual backend operations have finished.
#[derive(Clone)]
pub struct StorageRecoveryPermit(Arc<dyn Send + Sync>);

impl StorageRecoveryPermit {
    pub fn new<T: Send + Sync + 'static>(lease: T) -> Self {
        Self(Arc::new(lease))
    }
}

impl std::fmt::Debug for StorageRecoveryPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StorageRecoveryPermit(<held>)")
    }
}

impl PartialEq for StorageRecoveryPermit {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for StorageRecoveryPermit {}

pub type StorageRecoveryAdmission = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Option<StorageRecoveryPermit>> + Send>> + Send + Sync,
>;

/// Exact recovery work, captured before admission can reach the Writer. The watch and lifetime
/// survive request cancellation, including an acknowledgement lost before any file was created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageWriteRecovery {
    pub plan: StorageWritePlan,
    pub io: StorageIoWatch,
    /// Includes both the bounded queue slot and the exclusive node-lock lifetime. Verification
    /// jobs must retain this same resource while probing possible outstanding kernel work.
    pub lifetime: StorageRecoveryPermit,
}

/// The serving process supplies its committed generation, exclusive node lifetime and existing
/// retained recovery queue. There is no implicit generation or unbounded fallback for writes.
#[derive(Clone)]
pub struct StorageWriteRuntime {
    generation: StorageToken,
    node_lifetime: Arc<dyn Send + Sync>,
    admission: StorageRecoveryAdmission,
    recover: Arc<dyn Fn(StorageWriteRecovery) -> bool + Send + Sync>,
}

impl std::fmt::Debug for StorageWriteRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageWriteRuntime")
            .finish_non_exhaustive()
    }
}

impl StorageWriteRuntime {
    pub fn new(
        generation: StorageToken,
        node_lifetime: Arc<dyn Send + Sync>,
        admission: StorageRecoveryAdmission,
        recover: Arc<dyn Fn(StorageWriteRecovery) -> bool + Send + Sync>,
    ) -> Self {
        Self {
            generation,
            node_lifetime,
            admission,
            recover,
        }
    }

    pub(crate) async fn prepare(
        &self,
        blob: &dyn BlobStore,
        bucket: BucketName,
        target: StorageWriteTarget,
    ) -> Result<PreparedStorageWrite, Error> {
        let slot = (self.admission)().await.ok_or_else(|| {
            Error::Internal("storage recovery admission is unavailable".to_owned())
        })?;
        let planned = blob.plan_write(bucket, self.generation.clone(), target)?;
        let plan = planned.plan().clone();
        let lifetime = StorageRecoveryPermit::new((self.node_lifetime.clone(), slot));
        let (io, lease) = StorageIoWatch::new(
            plan.attempt.clone(),
            plan.generation.clone(),
            Arc::new(lifetime.clone()),
        );
        Ok(PreparedStorageWrite {
            planned,
            lease,
            guard: StorageWriteGuard {
                recovery: Some(StorageWriteRecovery { plan, io, lifetime }),
                recover: self.recover.clone(),
            },
        })
    }
}

pub(crate) struct PreparedStorageWrite {
    planned: PlannedStorageWrite,
    lease: StorageIoLease,
    guard: StorageWriteGuard,
}

impl PreparedStorageWrite {
    pub(crate) fn plan(&self) -> &StorageWritePlan {
        self.planned.plan()
    }

    pub(crate) fn admit(
        self,
        receipt: StorageAdmission,
    ) -> Result<(StorageCreationPermit, StorageWriteGuard), Error> {
        let permit = self.planned.admit(receipt, self.lease)?;
        Ok((permit, self.guard))
    }

    /// A typed admission miss owns no claim or files. An error or missing acknowledgement must
    /// instead leave the guard armed, because the Writer may already have committed admission.
    pub(crate) fn reject(mut self) {
        self.guard.disarm();
    }
}

pub(crate) struct StorageWriteGuard {
    recovery: Option<StorageWriteRecovery>,
    recover: Arc<dyn Fn(StorageWriteRecovery) -> bool + Send + Sync>,
}

impl StorageWriteGuard {
    pub(crate) fn publication(&self, operation: Mutation) -> Mutation {
        Mutation::PublishStorageWrite {
            plan: Box::new(
                self.recovery
                    .as_ref()
                    .expect("armed storage publication")
                    .plan
                    .clone(),
            ),
            operation: Box::new(operation),
        }
    }

    pub(crate) fn enqueue_recovery(&mut self) -> bool {
        let Some(recovery) = &self.recovery else {
            return true;
        };
        recovery.io.cancel();
        if (self.recover)(recovery.clone()) {
            self.recovery = None;
            true
        } else {
            false
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.recovery = None;
    }
}

impl Drop for StorageWriteGuard {
    fn drop(&mut self) {
        if let Some(recovery) = self.recovery.take() {
            recovery.io.cancel();
            let _ = (self.recover)(recovery);
        }
    }
}
