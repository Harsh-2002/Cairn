//! Explicit Writer admission for metadata-only fixtures. This module creates no physical files.
//! It never initializes a generation, rewrites an operation, retries or recovers implicitly.

use crate::storage::{
    PlannedStorageWrite, StorageAdmission, StorageIntentPath, StorageMutation, StoragePathRole,
    StorageToken, StorageWritePlan, StorageWriteTarget,
};
use crate::{
    BucketName, MetaError, MetadataStore, MultipartClaimToken, Mutation, MutationOutcome,
    ObjectVersionRow, StoragePath, Timestamp, UploadId,
};

/// One explicitly initialized storage generation shared by an isolated fixture's writers.
#[derive(Clone, Debug)]
pub struct PublicationFixture {
    generation: StorageToken,
}

impl Default for PublicationFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl PublicationFixture {
    /// Select a fresh generation without mutating a store. Call `begin` before admitting writes.
    #[must_use]
    pub fn new() -> Self {
        Self {
            generation: StorageToken::generate(),
        }
    }

    /// Use a generation already committed by the fixture's server/startup path.
    #[must_use]
    pub fn from_generation(generation: StorageToken) -> Self {
        Self { generation }
    }

    #[must_use]
    pub fn generation(&self) -> &StorageToken {
        &self.generation
    }

    /// Commit once per fixture/store (through the router for a sharded fixture).
    pub async fn begin<M: MetadataStore + ?Sized>(&self, meta: &M) -> Result<(), MetaError> {
        match meta
            .submit(Mutation::BeginStorageGeneration {
                generation: self.generation.clone(),
            })
            .await?
        {
            MutationOutcome::Ack => Ok(()),
            _ => Err(invalid("unexpected fixture generation outcome")),
        }
    }

    /// Plan the caller's existing canonical row/path. Exact-row and path assertions remain valid.
    pub fn object_plan(&self, row: &ObjectVersionRow) -> Result<StorageWritePlan, MetaError> {
        self.row_plan(
            row,
            StorageWriteTarget::Object {
                key: row.key.clone(),
                version_id: row.version_id.clone(),
                row_id: row.id.clone(),
            },
        )
    }

    /// Build this before the original completion claim, retaining it through publication/replay.
    pub fn completion_plan(
        &self,
        row: &ObjectVersionRow,
        upload_id: &UploadId,
        claim_token: &MultipartClaimToken,
    ) -> Result<StorageWritePlan, MetaError> {
        self.row_plan(
            row,
            StorageWriteTarget::Completion {
                upload_id: upload_id.clone(),
                claim_token: claim_token.as_str().to_owned(),
                key: row.key.clone(),
                version_id: row.version_id.clone(),
                row_id: row.id.clone(),
            },
        )
    }

    /// Build this before the original quota reservation. Part names retain its exact attempt id.
    pub fn part_plan(
        &self,
        bucket: &BucketName,
        upload_id: &UploadId,
        part_number: u16,
        reservation_id: &str,
    ) -> Result<StorageWritePlan, MetaError> {
        Ok(PlannedStorageWrite::new(
            bucket.clone(),
            self.generation.clone(),
            StorageWriteTarget::Part {
                upload_id: upload_id.clone(),
                part_number,
                reservation_id: reservation_id.to_owned(),
            },
        )?
        .plan()
        .clone())
    }

    fn row_plan(
        &self,
        row: &ObjectVersionRow,
        target: StorageWriteTarget,
    ) -> Result<StorageWritePlan, MetaError> {
        let path = row
            .storage_path
            .as_ref()
            .ok_or_else(|| invalid("fixture object requires a physical path"))?;
        let (bucket, name) = path
            .as_str()
            .split_once('/')
            .ok_or_else(|| invalid("fixture object requires a canonical flat path"))?;
        if bucket != row.bucket.as_str() {
            return Err(invalid("fixture object path belongs to another bucket"));
        }
        let attempt = StorageToken::try_from(name.to_owned())?;
        let plan = StorageWritePlan {
            attempt,
            generation: self.generation.clone(),
            bucket: row.bucket.clone(),
            target,
            paths: vec![
                StorageIntentPath {
                    role: StoragePathRole::Temporary,
                    path: StoragePath::from_string(format!(".staging/{name}.tmp")),
                },
                StorageIntentPath {
                    role: StoragePathRole::Final,
                    path: path.clone(),
                },
                StorageIntentPath {
                    role: StoragePathRole::IndexSpool,
                    path: StoragePath::from_string(format!(".staging/{name}.index.tmp")),
                },
            ],
        };
        plan.validate()?;
        Ok(plan)
    }

    /// Admit a planned physical fixture before handing its real creation permit to the blob store.
    /// The caller still publishes its row with `publication`; no files or cleanup are implicit.
    pub async fn admit_object<M: MetadataStore + ?Sized>(
        &self,
        meta: &M,
        planned: PlannedStorageWrite,
        now: Timestamp,
    ) -> Result<(StorageWritePlan, crate::storage::StorageCreationPermit), MetaError> {
        let plan = planned.plan().clone();
        if plan.generation != self.generation
            || !matches!(plan.target, StorageWriteTarget::Object { .. })
        {
            return Err(invalid("physical fixture plan generation/target mismatch"));
        }
        let outcome = meta
            .submit(Mutation::Storage {
                bucket: plan.bucket.clone(),
                operation: StorageMutation::Reserve {
                    plan: Box::new(plan.clone()),
                    now,
                },
            })
            .await?;
        let MutationOutcome::StorageAdmission(receipt @ StorageAdmission::Granted(_)) = outcome
        else {
            return Err(invalid(
                "physical fixture storage admission was not granted",
            ));
        };
        let (_watch, lease) = crate::storage::io::StorageIoWatch::new(
            plan.attempt.clone(),
            plan.generation.clone(),
            std::sync::Arc::new(()),
        );
        let permit = planned
            .admit(receipt, lease)
            .map_err(|error| MetaError::Engine(error.to_string()))?;
        Ok((plan, permit))
    }

    /// Admit an ordinary PUT now and return its unchanged final mutation for a later/racing submit.
    /// Marker/pathless metadata fixtures need no physical admission and pass through unchanged.
    pub async fn prepare_put<M: MetadataStore + ?Sized>(
        &self,
        meta: &M,
        mutation: Mutation,
    ) -> Result<Mutation, MetaError> {
        let Mutation::PutObjectVersion { row, .. } = &mutation else {
            return Ok(mutation);
        };
        if row.storage_path.is_none() {
            return Ok(mutation);
        }
        let plan = self.object_plan(row)?;
        let outcome = meta
            .submit(Mutation::Storage {
                bucket: row.bucket.clone(),
                operation: StorageMutation::Reserve {
                    plan: Box::new(plan.clone()),
                    now: row.updated_at,
                },
            })
            .await?;
        match outcome {
            MutationOutcome::StorageAdmission(StorageAdmission::Granted(admitted))
                if *admitted == plan =>
            {
                Self::publication(plan, mutation)
            }
            _ => Err(invalid("fixture storage admission was not granted")),
        }
    }

    /// Ordinary fixture setup convenience. Multipart admission remains explicit at reserve/claim.
    pub async fn submit<M: MetadataStore + ?Sized>(
        &self,
        meta: &M,
        mutation: Mutation,
    ) -> Result<MutationOutcome, MetaError> {
        let mutation = self.prepare_put(meta, mutation).await?;
        meta.submit(mutation).await
    }

    /// Wrap the original multipart reserve/claim; callers observe the real admission outcome.
    pub fn admission(
        plan: StorageWritePlan,
        operation: Mutation,
        now: Timestamp,
    ) -> Result<Mutation, MetaError> {
        plan.validate_admission(&operation)?;
        Ok(Mutation::AdmitStorageWrite {
            plan: Box::new(plan),
            operation: Box::new(operation),
            now,
        })
    }

    /// Wrap the original final write without changing its data, preconditions or side effects.
    pub fn publication(plan: StorageWritePlan, operation: Mutation) -> Result<Mutation, MetaError> {
        plan.validate_publication(&operation)?;
        Ok(Mutation::PublishStorageWrite {
            plan: Box::new(plan),
            operation: Box::new(operation),
        })
    }
}

fn invalid(message: &str) -> MetaError {
    MetaError::Engine(message.to_owned())
}

/// Explicit convenience methods; ordinary `MetadataStore::submit` is never intercepted.
#[async_trait::async_trait]
pub trait FixtureMetadataStore: MetadataStore {
    /// Begin exactly one new generation for this isolated fixture.
    async fn begin_fixture(&self) -> Result<PublicationFixture, MetaError> {
        let fixture = PublicationFixture::new();
        fixture.begin(self).await?;
        Ok(fixture)
    }

    async fn submit_fixture(
        &self,
        fixture: &PublicationFixture,
        mutation: Mutation,
    ) -> Result<MutationOutcome, MetaError> {
        fixture.submit(self, mutation).await
    }
}

impl<M: MetadataStore + ?Sized> FixtureMetadataStore for M {}
