//! `cairn-control` — the JSON management API for Cairn (ARCH 22). It is the admin-gated
//! control surface, distinct from the S3 data plane: JSON over HTTP, versioned in its path
//! (`/api/v1`), and consumed by both the embedded web console and the command-line interface.
//!
//! The crate is written entirely against the trait spine in [`cairn_types::traits`]
//! ([`MetadataStore`], [`BlobStore`], [`Crypto`], [`Clock`]), so it is unit-testable against
//! the in-memory doubles in `cairn_types::testing` and carries no backend of its own. The
//! single entry point is [`ControlService::handle`], which routes a method + subpath (the part
//! after `/api/v1`) to the contract endpoints and returns a [`ControlResponse`] carrying an
//! HTTP status and a JSON body.
//!
//! [`MetadataStore`]: cairn_types::traits::MetadataStore
//! [`BlobStore`]: cairn_types::traits::BlobStore
//! [`Crypto`]: cairn_types::traits::Crypto
//! [`Clock`]: cairn_types::traits::Clock

#![forbid(unsafe_code)]

mod service;
mod wire;

pub use service::{ControlResponse, ControlService, SystemInfo, UpdateStatus};
