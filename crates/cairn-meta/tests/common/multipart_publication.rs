//! Capture each exact plan at the original multipart admission seam, before its Writer submit.
//! Final mutation helpers only replay that captured plan; they never claim or reserve lazily.

use cairn_types::storage::StorageWritePlan;
use cairn_types::testing::PublicationFixture;
use cairn_types::*;
use std::collections::BTreeMap;

pub struct MultipartPublication {
    fixture: PublicationFixture,
    parts: BTreeMap<(String, String), StorageWritePlan>,
    completions: BTreeMap<(String, String), StorageWritePlan>,
}

impl MultipartPublication {
    pub fn new(fixture: &PublicationFixture) -> Self {
        Self {
            fixture: fixture.clone(),
            parts: BTreeMap::new(),
            completions: BTreeMap::new(),
        }
    }

    pub async fn reserve_part(
        &mut self,
        meta: &dyn MetadataStore,
        operation: Mutation,
    ) -> Result<MutationOutcome, MetaError> {
        let Mutation::ReserveMultipartPart {
            upload_id,
            part_number,
            attempt_id,
            now,
            ..
        } = &operation
        else {
            panic!("original part reservation required")
        };
        let session = meta
            .get_multipart(upload_id)
            .await?
            .expect("part fixture has a session");
        let plan = self
            .fixture
            .part_plan(&session.bucket, upload_id, *part_number, attempt_id)?;
        self.parts.insert(
            (upload_id.as_str().to_owned(), attempt_id.clone()),
            plan.clone(),
        );
        let now = *now;
        meta.submit(PublicationFixture::admission(plan, operation, now)?)
            .await
    }

    pub async fn record_part(
        &self,
        meta: &dyn MetadataStore,
        operation: Mutation,
    ) -> Result<MutationOutcome, MetaError> {
        let Mutation::RecordPart {
            upload_id,
            attempt_id,
            ..
        } = &operation
        else {
            panic!("original part publication required")
        };
        let plan = self
            .parts
            .get(&(upload_id.as_str().to_owned(), attempt_id.clone()))
            .expect("record uses its original admission plan")
            .clone();
        meta.submit(PublicationFixture::publication(plan, operation)?)
            .await
    }

    pub async fn claim(
        &mut self,
        meta: &dyn MetadataStore,
        operation: Mutation,
        row: &ObjectVersionRow,
    ) -> Result<MutationOutcome, MetaError> {
        let Mutation::ClaimMultipart {
            upload_id,
            claim_token,
        } = &operation
        else {
            panic!("original completion claim required")
        };
        let plan = self.fixture.completion_plan(row, upload_id, claim_token)?;
        self.completions.insert(
            (
                upload_id.as_str().to_owned(),
                claim_token.as_str().to_owned(),
            ),
            plan.clone(),
        );
        meta.submit(PublicationFixture::admission(
            plan,
            operation,
            row.updated_at,
        )?)
        .await
    }

    pub async fn complete(
        &self,
        meta: &dyn MetadataStore,
        operation: Mutation,
    ) -> Result<MutationOutcome, MetaError> {
        let Mutation::CompleteMultipart {
            upload_id,
            claim_token,
            ..
        } = &operation
        else {
            panic!("original completion publication required")
        };
        let plan = self
            .completions
            .get(&(
                upload_id.as_str().to_owned(),
                claim_token.as_str().to_owned(),
            ))
            .expect("completion uses its original admission plan")
            .clone();
        meta.submit(PublicationFixture::publication(plan, operation)?)
            .await
    }
}
