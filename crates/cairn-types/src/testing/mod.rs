//! Canonical in-memory test doubles for every trait in the spine. Downstream crates enable
//! `cairn-types/testing` as a dev-dependency and test their handlers against these, so the
//! whole engine is unit-testable in milliseconds without a disk or a database.

mod blob;
mod clock;
mod crypto;
mod meta;
mod replication;

pub use blob::InMemoryBlobStore;
pub use clock::TestClock;
pub use crypto::{StubCrypto, StubPublicUrl};
pub use meta::{InMemoryMetadataStore, SetReconcileOracle};
pub use replication::{FakeReplicationSink, RecordedIntent, SinkBehavior};

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
