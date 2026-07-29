//! Shared outbound-network safety for Cairn.
//!
//! Cairn dials remote endpoints that an **operator supplies**: replication targets, webhook
//! receivers, and (new) S3 import sources. Left unguarded, such a dialer is a Server-Side Request
//! Forgery (SSRF) primitive — an admin (or an attacker who reached the admin API) could point it at
//! `http://169.254.169.254/…` (the cloud metadata service), a loopback admin port, or an internal
//! RFC1918 service, and read the response back or use the request as a probe.
//!
//! This crate is the one place that decides *what an outbound connection may reach*. It exposes:
//!
//! - [`ip_is_internal`] — the pure predicate: is this resolved IP one we refuse to dial?
//! - [`GuardedHttpConnector`] / [`guarded_http_connector`] — the **connect-time** guarantee: it
//!   rejects blocked IP-literal URI hosts before Hyper can bypass DNS, then delegates hostname dials
//!   to [`GuardedResolver`], which rejects the whole resolved address set if *any* entry is internal.
//!   The resolver runs on every dial and therefore also defeats DNS rebinding.
//! - [`validate_endpoint`] — the **validate-time** layer: a fast, operator-visible check at
//!   configuration time (e.g. when creating an import job) so a bad endpoint is rejected with a clear
//!   message instead of failing later mid-run. It is defence-in-depth/UX, not the guarantee — the
//!   connector is.
//!
//! The escape hatch is [`GuardConfig::allow_internal`] (wired to `CAIRN_ALLOW_INTERNAL_ENDPOINTS`),
//! which an on-prem operator running against RFC1918 storage must set deliberately.

mod ssrf;

pub use ssrf::{
    GuardConfig, GuardedHttpConnector, GuardedResolver, SsrfError, guarded_http_connector,
    ip_is_internal, validate_endpoint,
};
