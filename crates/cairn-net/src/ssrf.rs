//! The SSRF guard: an internal-address predicate plus the connect-time resolver that enforces it.

use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::connect::dns::{GaiResolver, Name};
use tower_service::Service;

/// Policy for the guard: normally enforcing, but an operator running Cairn against storage on a
/// private network (an on-prem MinIO on `10.x`, say) can flip [`allow_internal`](Self::allow_internal)
/// on via `CAIRN_ALLOW_INTERNAL_ENDPOINTS=true`. It is a single `Copy` bool so validate-time and
/// connect-time always agree.
#[derive(Debug, Clone, Copy, Default)]
pub struct GuardConfig {
    /// When true the guard is disabled — every address is permitted. Default (`false`) is enforcing.
    pub allow_internal: bool,
}

impl GuardConfig {
    /// Construct a policy; `allow_internal == false` is the enforcing default.
    #[must_use]
    pub fn new(allow_internal: bool) -> Self {
        Self { allow_internal }
    }
}

/// Whether `ip` is an address Cairn refuses to dial (loopback, private, link-local, ULA,
/// unspecified, multicast, and IPv4-mapped/compatible/NAT64 forms of those). Returns `false` for
/// every address when [`GuardConfig::allow_internal`] is set.
///
/// This is the single source of truth for "internal"; both the connect-time [`GuardedResolver`] and
/// the validate-time [`validate_endpoint`] call it, so they can never disagree.
#[must_use]
pub fn ip_is_internal(ip: IpAddr, cfg: &GuardConfig) -> bool {
    if cfg.allow_internal {
        return false;
    }
    match ip {
        IpAddr::V4(v4) => ipv4_is_internal(v4),
        IpAddr::V6(v6) => ipv6_is_internal(v6),
    }
}

fn ipv4_is_internal(ip: Ipv4Addr) -> bool {
    ip.is_loopback()            // 127.0.0.0/8
        || ip.is_private()      // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local()   // 169.254.0.0/16 (cloud metadata lives here)
        || ip.is_broadcast()    // 255.255.255.255
        || ip.is_documentation()// 192.0.2/24, 198.51.100/24, 203.0.113/24 (not real hosts)
        || ip.is_multicast()    // 224.0.0.0/4
        || ip.octets()[0] == 0  // 0.0.0.0/8 "this network" (includes 0.0.0.0 unspecified)
        || v4_in(ip, [100, 64, 0, 0], 10) // 100.64.0.0/10 carrier-grade NAT
        || v4_in(ip, [192, 0, 0, 0], 24) // 192.0.0.0/24 IETF protocol assignments
}

fn ipv6_is_internal(ip: Ipv6Addr) -> bool {
    // Unwrap IPv4-mapped (`::ffff:a.b.c.d`) and the deprecated IPv4-compatible (`::a.b.c.d`) forms
    // and re-check as IPv4 — otherwise `::ffff:169.254.169.254` would sail past the v6 range checks
    // and reach the metadata service. `to_ipv4()` also covers the compatible range.
    if let Some(v4) = ip.to_ipv4_mapped().or_else(|| ip.to_ipv4()) {
        if ipv4_is_internal(v4) {
            return true;
        }
    }
    ip.is_loopback()                        // ::1
        || ip.is_unspecified()              // ::
        || ip.is_multicast()                // ff00::/8
        || v6_in(ip, 0xfc00_u128 << 112, 7) // fc00::/7 unique local
        || v6_in(ip, 0xfe80_u128 << 112, 10) // fe80::/10 link-local
        || nat64_embeds_internal(ip) // 64:ff9b::/96 -> check the embedded IPv4
}

/// 64:ff9b::/96 (RFC 6052 NAT64) embeds an IPv4 address in its low 32 bits; a NAT64 prefix pointing
/// at an internal v4 is the same bypass as an IPv4-mapped address, so check the embedded octets.
fn nat64_embeds_internal(ip: Ipv6Addr) -> bool {
    let nat64 = 0x0064_ff9b_u128 << 96;
    if !v6_in(ip, nat64, 96) {
        return false;
    }
    let v4 = Ipv4Addr::from((u128::from(ip) & 0xffff_ffff) as u32);
    ipv4_is_internal(v4)
}

fn v4_in(ip: Ipv4Addr, net: [u8; 4], prefix: u32) -> bool {
    let ip = u32::from(ip);
    let net = u32::from(Ipv4Addr::new(net[0], net[1], net[2], net[3]));
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    (ip & mask) == (net & mask)
}

fn v6_in(ip: Ipv6Addr, net: u128, prefix: u32) -> bool {
    let ip = u128::from(ip);
    let mask = if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    };
    (ip & mask) == (net & mask)
}

/// A DNS resolver that wraps the standard [`GaiResolver`] and rejects a lookup whose result set
/// contains **any** blocked address (see [`ip_is_internal`]). Rejecting the whole set — rather than
/// filtering out the internal entries and connecting to the rest — is deliberate: a hostname that
/// resolves to a mix of public and internal addresses is itself an attack signature, and
/// happy-eyeballs could otherwise still land on the internal one.
///
/// Because hyper's connector runs this on **every** dial (initial connect, redirect, reconnect), it
/// is the layer that defeats DNS rebinding: it checks the exact addresses the connection will use,
/// not a resolution taken earlier at validation time.
#[derive(Debug, Clone)]
pub struct GuardedResolver {
    inner: GaiResolver,
    cfg: GuardConfig,
}

impl GuardedResolver {
    /// Wrap a fresh [`GaiResolver`] with the given policy.
    #[must_use]
    pub fn new(cfg: GuardConfig) -> Self {
        Self {
            inner: GaiResolver::new(),
            cfg,
        }
    }
}

impl Service<Name> for GuardedResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let cfg = self.cfg;
        let host = name.as_str().to_owned();
        let lookup = Service::call(&mut self.inner, name);
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = lookup.await?.collect();
            for addr in &addrs {
                if ip_is_internal(addr.ip(), &cfg) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "refused outbound connection to {host}: resolves to blocked internal \
                             address {} (set CAIRN_ALLOW_INTERNAL_ENDPOINTS=true to allow)",
                            addr.ip()
                        ),
                    ));
                }
            }
            Ok(addrs.into_iter())
        })
    }
}

/// Build an [`HttpConnector`] whose DNS goes through the [`GuardedResolver`], ready to be handed to
/// `hyper_rustls::HttpsConnectorBuilder::…wrap_connector(…)`. `enforce_http(false)` lets the inner
/// connector accept `https://` URIs (the rustls layer handles TLS on top of it).
#[must_use]
pub fn guarded_http_connector(cfg: GuardConfig) -> HttpConnector<GuardedResolver> {
    let mut http = HttpConnector::new_with_resolver(GuardedResolver::new(cfg));
    http.enforce_http(false);
    http
}

/// The validate-time layer: reject `endpoint` at configuration time if its host is an **IP literal**
/// that is internal (e.g. `http://127.0.0.1:9000`, `https://169.254.169.254`, `http://[::1]`). This
/// gives immediate, operator-facing feedback for the common SSRF attempt without touching DNS — so it
/// is synchronous and adds no network dependency or latency to a config write, and keeps the control
/// plane's unit tests hermetic.
///
/// It deliberately does **not** resolve hostnames: a hostname that maps to an internal address is
/// stopped by [`GuardedResolver`] at connect time (which also defeats DNS rebinding). Validate-time is
/// defence-in-depth/UX; the connector is the guarantee.
///
/// # Errors
/// [`SsrfError::Blocked`] if the host is an internal IP literal. A hostname, a hostless URL, or an
/// unparseable one returns `Ok` here (not this layer's concern — the connector and the caller's own
/// field validation cover those).
pub fn validate_endpoint(endpoint: &str, cfg: &GuardConfig) -> Result<(), SsrfError> {
    if cfg.allow_internal {
        return Ok(());
    }
    let Ok(uri) = endpoint.parse::<http::Uri>() else {
        return Ok(());
    };
    let Some(raw_host) = uri.host() else {
        return Ok(());
    };
    // `Uri::host()` keeps the brackets on an IPv6 literal (`[::1]`); strip them before parsing.
    let host = raw_host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(raw_host);
    if let Ok(ip) = host.parse::<IpAddr>() {
        if ip_is_internal(ip, cfg) {
            return Err(SsrfError::Blocked {
                host: host.to_owned(),
                ip,
            });
        }
    }
    Ok(())
}

/// A validate-time SSRF rejection.
#[derive(Debug, thiserror::Error)]
pub enum SsrfError {
    /// The endpoint's host is a blocked internal IP literal.
    #[error(
        "endpoint {host} is a blocked internal address ({ip}); \
         set CAIRN_ALLOW_INTERNAL_ENDPOINTS=true to allow it"
    )]
    Blocked {
        /// The endpoint host.
        host: String,
        /// The blocked address.
        ip: IpAddr,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enforce() -> GuardConfig {
        GuardConfig::new(false)
    }

    fn internal(s: &str) -> bool {
        ip_is_internal(s.parse().unwrap(), &enforce())
    }

    #[test]
    fn blocks_the_ipv4_internal_ranges() {
        for s in [
            "127.0.0.1",
            "127.1.2.3",
            "10.0.0.5",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.169.254", // cloud metadata
            "0.0.0.0",
            "0.1.2.3",
            "100.64.0.1", // CGNAT
            "192.0.0.1",  // IETF protocol
            "255.255.255.255",
            "224.0.0.1", // multicast
        ] {
            assert!(internal(s), "{s} should be blocked");
        }
    }

    #[test]
    fn allows_public_ipv4() {
        // Includes the just-outside-range neighbours of 172.16/12 to prove the mask is exact.
        for s in [
            "8.8.8.8",
            "1.1.1.1",
            "93.184.216.34",
            "172.15.0.1",
            "172.32.0.1",
        ] {
            assert!(!internal(s), "{s} should be allowed");
        }
    }

    #[test]
    fn blocks_ipv6_internal_and_mapped_forms() {
        for s in [
            "::1",                    // loopback
            "::",                     // unspecified
            "fc00::1",                // ULA
            "fd12:3456::1",           // ULA
            "fe80::1",                // link-local
            "ff02::1",                // multicast
            "::ffff:127.0.0.1",       // IPv4-mapped loopback
            "::ffff:169.254.169.254", // IPv4-mapped metadata — the classic bypass
            "::ffff:10.0.0.1",        // IPv4-mapped private
            "64:ff9b::7f00:1",        // NAT64 embedding 127.0.0.1
            "64:ff9b::a9fe:a9fe",     // NAT64 embedding 169.254.169.254
        ] {
            assert!(internal(s), "{s} should be blocked");
        }
    }

    #[test]
    fn allows_public_ipv6_and_public_mapped() {
        for s in [
            "2606:4700:4700::1111", // public (Cloudflare)
            "2001:4860:4860::8888", // public (Google)
            "::ffff:8.8.8.8",       // IPv4-mapped public
            "64:ff9b::8080:8080",   // NAT64 embedding 128.128.128.128 (public)
        ] {
            assert!(!internal(s), "{s} should be allowed");
        }
    }

    #[test]
    fn escape_hatch_allows_everything() {
        let permissive = GuardConfig::new(true);
        for s in ["127.0.0.1", "169.254.169.254", "::1", "::ffff:10.0.0.1"] {
            assert!(!ip_is_internal(s.parse().unwrap(), &permissive));
        }
    }

    #[test]
    fn validate_endpoint_rejects_literal_internal() {
        for ep in [
            "http://127.0.0.1:9000",
            "https://169.254.169.254",
            "http://[::1]:7373",
            "http://[::ffff:10.0.0.1]:80",
        ] {
            let err = validate_endpoint(ep, &enforce()).unwrap_err();
            assert!(matches!(err, SsrfError::Blocked { .. }), "{ep} -> {err:?}");
        }
    }

    #[test]
    fn validate_endpoint_allows_public_literal_and_hostnames() {
        // Public IP literals pass; hostnames are deferred to the connect-time resolver (Ok here).
        for ep in [
            "https://8.8.8.8",
            "https://peer.example.com:9000",
            "http://minio.internal:9000",
        ] {
            assert!(validate_endpoint(ep, &enforce()).is_ok(), "{ep}");
        }
    }

    #[test]
    fn validate_endpoint_escape_hatch() {
        let permissive = GuardConfig::new(true);
        assert!(validate_endpoint("http://127.0.0.1:9000", &permissive).is_ok());
    }
}
