//! `cairn-lifecycle` — the background lifecycle scanner (ARCH 19).
//!
//! This crate parses an S3 `<LifecycleConfiguration>` into typed rules ([`parse_lifecycle`])
//! and applies the due actions with an idempotent scanner ([`LifecycleScanner`]) written
//! entirely against the `cairn-types` trait spine ([`cairn_types::MetadataStore`],
//! [`cairn_types::BlobStore`], [`cairn_types::Clock`]). Because it depends only on those
//! traits it is unit-testable against the in-memory doubles, and the same code drives the real
//! SQLite/filesystem backends in production.
//!
//! # Supported actions
//! - **Expiration** of a current object after N days from creation or on a date. In an
//!   unversioned bucket this permanently deletes the object and reclaims its blob; in a
//!   versioning-enabled bucket it inserts a delete marker (ARCH 19.3).
//! - **Noncurrent-version expiration** of versions older than N days, preserving a configurable
//!   number of the newest noncurrent versions.
//! - **Expired-object-delete-marker removal** once a delete marker is the only remaining
//!   version of a key.
//! - **Aborting incomplete multipart uploads** N days after initiation, reclaiming staged parts
//!   (ARCH 19.4).
//!
//! # Cold-tier transition
//! Transition of objects to a remote cold tier (ARCH 19.5) is parsed but treated as a
//! documented **NO-OP placeholder** in v1: the scanner recognizes the action and performs no
//! data movement. The fully-implementable expiration/abort actions above are the focus.

#![forbid(unsafe_code)]

mod config;
mod scanner;

pub use config::{Action, Expiration, Filter, LifecycleRule, Transition, parse_lifecycle};
pub use scanner::{BucketLifecycle, LifecycleReport, LifecycleScanner};

#[cfg(test)]
mod tests;
