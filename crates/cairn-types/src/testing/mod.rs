//! Canonical in-memory test doubles for every trait in the spine. Downstream crates enable
//! `cairn-types/testing` as a dev-dependency and test their handlers against these, so the
//! whole engine is unit-testable in milliseconds without a disk or a database.

mod blob;
mod clock;
mod crypto;
mod meta;
mod publication_fixture;
mod replication;

pub use blob::{FixtureBlobStore, InMemoryBlobStore, fixture_storage_cleanup, fixture_storage_io};
pub use clock::TestClock;
pub use crypto::{StubCrypto, StubPublicUrl};
pub use meta::{InMemoryMetadataStore, SetReconcileOracle};
pub use publication_fixture::{FixtureMetadataStore, PublicationFixture};
pub use replication::{
    FakeReplicationSink, RecordedIntent, ReplicationClaims, SinkBehavior,
    assert_replication_claim_fencing,
};

use crate::auth::{AuthOutcome, Principal, RequestView};
use crate::authz::{AuthzInput, Decision, DenyReason};
use crate::traits::{Authenticator, AuthorizationEngine};

/// An authenticator that always yields a fixed principal (or `NotApplicable` if `None`).
#[derive(Debug, Clone)]
pub struct FixedAuthenticator(pub Option<Principal>);

#[async_trait::async_trait]
impl Authenticator for FixedAuthenticator {
    async fn authenticate(&self, _view: &RequestView<'_>) -> AuthOutcome {
        match &self.0 {
            Some(p) => AuthOutcome::Authenticated(p.clone()),
            None => AuthOutcome::NotApplicable,
        }
    }
}

/// An authorization engine that allows everything (for handler tests not about authz).
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl AuthorizationEngine for AllowAll {
    fn evaluate(&self, _input: &AuthzInput) -> Decision {
        Decision::Allow
    }
}

/// An authorization engine that denies everything.
#[derive(Debug, Clone, Copy, Default)]
pub struct DenyAll;

impl AuthorizationEngine for DenyAll {
    fn evaluate(&self, _input: &AuthzInput) -> Decision {
        Decision::Deny(DenyReason::DefaultDeny)
    }
}

mod replication_upload_contract;
pub use replication_upload_contract::assert_replication_upload_journal;

mod storage_contract;
pub use storage_contract::assert_storage_journal;
