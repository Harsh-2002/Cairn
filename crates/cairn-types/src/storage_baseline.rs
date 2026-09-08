//! Offline storage coverage and conservative legacy-accounting ownership (ARCH 8.5).

use crate::storage::StorageToken;
use crate::{BucketName, ObjectVersionRow, PartRecord, StoragePath, Timestamp, UploadId};

pub const STORAGE_BASELINE_PAGE_LIMIT: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageBaselineToken {
    pub generation: StorageToken,
    pub baseline_id: StorageToken,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageBaselineState {
    pub generation: Option<StorageToken>,
    pub coverage_identity: Option<StorageToken>,
    pub completed_at: Option<Timestamp>,
    pub baseline_id: Option<StorageToken>,
    pub legacy_accounting_hold: bool,
    pub legacy_release_authorized: bool,
}

impl StorageBaselineState {
    pub fn matches(&self, token: &StorageBaselineToken) -> bool {
        self.generation.as_ref() == Some(&token.generation)
            && self.baseline_id.as_ref() == Some(&token.baseline_id)
            && self.legacy_accounting_hold
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageBaselinePending {
    pub intents: bool,
    pub intent_paths: bool,
    pub exact_debt: bool,
    pub native_quota_debt: bool,
    pub legacy_reservations: bool,
    pub legacy_quota_debt: bool,
}

impl StorageBaselinePending {
    pub fn native_pending(&self) -> bool {
        self.intents || self.intent_paths || self.exact_debt || self.native_quota_debt
    }
    pub fn any(&self) -> bool {
        self.native_pending() || self.legacy_reservations || self.legacy_quota_debt
    }
    pub fn merge(&mut self, other: &Self) {
        self.intents |= other.intents;
        self.intent_paths |= other.intent_paths;
        self.exact_debt |= other.exact_debt;
        self.native_quota_debt |= other.native_quota_debt;
        self.legacy_reservations |= other.legacy_reservations;
        self.legacy_quota_debt |= other.legacy_quota_debt;
    }
}

/// One result per requested path, in exactly the requested order. Conflicting owners are errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePathOwnership {
    pub path: StoragePath,
    pub bucket: Option<BucketName>,
    pub authoritative: bool,
    pub intent: bool,
    pub cleanup: bool,
    pub legacy_debt: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StorageAuthorityKind {
    #[default]
    Objects,
    Parts,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageAuthorityCursor {
    pub shard: u32,
    pub kind: StorageAuthorityKind,
    pub last_id: String,
    pub last_part: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageAuthority {
    Object(Box<ObjectVersionRow>),
    Part {
        bucket: BucketName,
        upload_id: UploadId,
        part: PartRecord,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageAuthorityPage {
    pub items: Vec<StorageAuthority>,
    pub next: Option<StorageAuthorityCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBaselineDisposition {
    Authoritative,
    IntentOwned,
    CleanupRecorded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBaselineTransition {
    Applied,
    AlreadyApplied,
    Stale,
    Blocked,
}
