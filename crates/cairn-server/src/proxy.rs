//! Trusted reverse-proxy configuration and request provenance.
//!
//! Forwarded headers are attacker-controlled unless the TCP peer that delivered them is explicitly
//! trusted. This module keeps that decision in one place so transport-derived decisions (the
//! console cookie and `aws:SourceIp`) cannot grow subtly different parser or trust rules.

use cairn_types::auth::ClientSource;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};

const MAX_TRUSTED_NETWORKS: usize = 64;
const MAX_FORWARDED_HOPS: usize = 32;

/// A validated, immutable allow-list of reverse-proxy source networks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TrustedProxies {
    networks: Vec<IpNetwork>,
}

impl TrustedProxies {
    /// Parse `CAIRN_TRUSTED_PROXIES`.
    ///
    /// Entries are comma-separated exact IP addresses or canonical CIDR networks. Empty entries,
    /// hostnames, non-canonical networks, mapped-IPv6 CIDRs, and trust-everything `/0` networks are
    /// rejected so a typo never silently widens the trust boundary.
    pub(crate) fn parse(raw: Option<&str>) -> Result<Self, String> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        if raw.trim().is_empty() {
            return Err(
                "must not be empty; unset CAIRN_TRUSTED_PROXIES to trust no proxies".into(),
            );
        }

        let entries: Vec<&str> = raw.split(',').map(str::trim).collect();
        if entries.len() > MAX_TRUSTED_NETWORKS {
            return Err(format!(
                "contains {} entries; at most {MAX_TRUSTED_NETWORKS} are allowed",
                entries.len()
            ));
        }
        if entries.iter().any(|entry| entry.is_empty()) {
            return Err("contains an empty entry".into());
        }

        let mut networks = Vec::with_capacity(entries.len());
        for entry in entries {
            let network = IpNetwork::parse(entry)?;
            if networks.contains(&network) {
                return Err(format!("contains duplicate entry {entry:?}"));
            }
            networks.push(network);
        }
        Ok(Self { networks })
    }

    pub(crate) fn contains(&self, address: IpAddr) -> bool {
        let address = canonical_ip(address);
        self.networks
            .iter()
            .any(|network| network.contains(address))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IpNetwork {
    address: IpAddr,
    prefix: u8,
}

impl IpNetwork {
    fn parse(raw: &str) -> Result<Self, String> {
        let (address_raw, prefix_raw) = match raw.split_once('/') {
            Some((address, prefix)) => (address, Some(prefix)),
            None => (raw, None),
        };
        if address_raw.is_empty()
            || prefix_raw.is_some_and(|prefix| prefix.is_empty() || prefix.contains('/'))
        {
            return Err(format!("{raw:?} is not an IP address or CIDR network"));
        }

        let parsed_address = address_raw
            .parse::<IpAddr>()
            .map_err(|_| format!("{raw:?} is not an IP address or CIDR network"))?;
        if matches!(parsed_address, IpAddr::V6(address) if address.to_ipv4_mapped().is_some())
            && prefix_raw.is_some()
        {
            return Err(format!(
                "{raw:?} is an IPv4-mapped IPv6 CIDR; use its IPv4 network"
            ));
        }
        let address = canonical_ip(parsed_address);
        if address.is_unspecified() && prefix_raw.is_none() {
            return Err(format!(
                "{raw:?} is unspecified and can never be a TCP peer"
            ));
        }

        let bits = match address {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        let prefix = match prefix_raw {
            Some(prefix) => prefix
                .parse::<u8>()
                .map_err(|_| format!("{raw:?} has an invalid prefix length"))?,
            None => bits,
        };
        if prefix == 0 {
            return Err(format!(
                "{raw:?} trusts every address; a /0 trusted-proxy network is forbidden"
            ));
        }
        if prefix > bits {
            return Err(format!(
                "{raw:?} has prefix /{prefix}, but this address family has only {bits} bits"
            ));
        }

        let network_address = mask_address(address, prefix);
        if prefix_raw.is_some() && network_address != address {
            return Err(format!(
                "{raw:?} is not a canonical network; use {network_address}/{prefix}"
            ));
        }
        Ok(Self {
            address: network_address,
            prefix,
        })
    }

    fn contains(self, address: IpAddr) -> bool {
        match (self.address, address) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                let mask = ipv4_mask(self.prefix);
                u32::from(address) & mask == u32::from(network)
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                let mask = ipv6_mask(self.prefix);
                u128::from(address) & mask == u128::from(network)
            }
            _ => false,
        }
    }
}

fn canonical_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) => address
            .to_ipv4_mapped()
            .map_or(IpAddr::V6(address), IpAddr::V4),
        address => address,
    }
}

fn mask_address(address: IpAddr, prefix: u8) -> IpAddr {
    match address {
        IpAddr::V4(address) => IpAddr::V4((u32::from(address) & ipv4_mask(prefix)).into()),
        IpAddr::V6(address) => IpAddr::V6((u128::from(address) & ipv6_mask(prefix)).into()),
    }
}

fn ipv4_mask(prefix: u8) -> u32 {
    u32::MAX.checked_shl(u32::from(32 - prefix)).unwrap_or(0)
}

fn ipv6_mask(prefix: u8) -> u128 {
    u128::MAX.checked_shl(u32::from(128 - prefix)).unwrap_or(0)
}

/// The externally-visible request scheme established from authenticated transport provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectiveScheme {
    Http,
    Https,
}

impl EffectiveScheme {
    pub(crate) const fn is_https(self) -> bool {
        matches!(self, Self::Https)
    }
}

/// Determine the external scheme without trusting a client-supplied header.
///
/// Direct TLS is authoritative. On plaintext connections, forwarding metadata is considered only
/// when the immediate TCP peer is allow-listed. Malformed or contradictory client-address
/// provenance, conflicting `Forwarded`/`X-Forwarded-Proto` schemes, or an unverifiable chain
/// resolves to HTTP (fail closed).
pub(crate) fn effective_scheme(
    direct_tls: bool,
    immediate_peer: IpAddr,
    headers: &[(String, String)],
    trusted_proxies: &TrustedProxies,
) -> EffectiveScheme {
    if direct_tls {
        return EffectiveScheme::Https;
    }
    if !trusted_proxies.contains(immediate_peer) {
        return EffectiveScheme::Http;
    }

    let forwarded = forwarded_boundary(headers, immediate_peer, trusted_proxies);
    let x_forwarded_for = x_forwarded_for(headers, immediate_peer, trusted_proxies);
    if !source_headers_agree(forwarded, x_forwarded_for) {
        return EffectiveScheme::Http;
    }
    let forwarded_scheme = match forwarded {
        HeaderProvenance::Absent => HeaderProvenance::Absent,
        HeaderProvenance::Valid(boundary) => boundary
            .scheme
            .map_or(HeaderProvenance::Absent, HeaderProvenance::Valid),
        HeaderProvenance::Invalid => HeaderProvenance::Invalid,
    };
    let x_forwarded = x_forwarded_proto(headers);
    match (forwarded_scheme, x_forwarded) {
        (HeaderProvenance::Absent, HeaderProvenance::Absent) => EffectiveScheme::Http,
        (HeaderProvenance::Valid(scheme), HeaderProvenance::Absent)
        | (HeaderProvenance::Absent, HeaderProvenance::Valid(scheme)) => scheme,
        (HeaderProvenance::Valid(left), HeaderProvenance::Valid(right)) if left == right => left,
        // One malformed family poisons the provenance even when the other says HTTPS. Otherwise a
        // proxy/client disagreement could be resolved in the privilege-increasing direction.
        _ => EffectiveScheme::Http,
    }
}

/// Resolve the requester's address from socket and forwarding provenance.
///
/// Headers from an untrusted immediate peer are ignored in their entirety. Once that peer is
/// trusted, however, Cairn must establish an explicit client boundary: absent, malformed, or
/// contradictory provenance is represented as [`ClientSource::Unavailable`] rather than silently
/// authorizing the proxy's address.
pub(crate) fn client_source(
    immediate_peer: IpAddr,
    headers: &[(String, String)],
    trusted_proxies: &TrustedProxies,
) -> ClientSource {
    let immediate_peer = canonical_ip(immediate_peer);
    if !trusted_proxies.contains(immediate_peer) {
        return ClientSource::Direct(immediate_peer);
    }

    let forwarded = forwarded_boundary(headers, immediate_peer, trusted_proxies);
    let x_forwarded = x_forwarded_for(headers, immediate_peer, trusted_proxies);
    match (forwarded, x_forwarded) {
        (HeaderProvenance::Valid(boundary), HeaderProvenance::Absent) => {
            ClientSource::Forwarded(boundary.client_address)
        }
        (HeaderProvenance::Absent, HeaderProvenance::Valid(address)) => {
            ClientSource::Forwarded(address)
        }
        (HeaderProvenance::Valid(boundary), HeaderProvenance::Valid(address))
            if boundary.client_address == address =>
        {
            ClientSource::Forwarded(address)
        }
        _ => ClientSource::Unavailable,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderProvenance<T> {
    Absent,
    Valid(T),
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ForwardedHop {
    for_address: IpAddr,
    scheme: Option<EffectiveScheme>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ForwardedBoundary {
    client_address: IpAddr,
    scheme: Option<EffectiveScheme>,
}

fn forwarded_boundary(
    headers: &[(String, String)],
    immediate_peer: IpAddr,
    trusted_proxies: &TrustedProxies,
) -> HeaderProvenance<ForwardedBoundary> {
    let values: Vec<&str> = header_values(headers, "forwarded").collect();
    if values.is_empty() {
        return HeaderProvenance::Absent;
    }

    let mut hops = Vec::new();
    for value in values {
        let elements = match split_quoted(value, ',') {
            Ok(elements) => elements,
            Err(()) => return HeaderProvenance::Invalid,
        };
        for element in elements {
            if hops.len() == MAX_FORWARDED_HOPS {
                return HeaderProvenance::Invalid;
            }
            match parse_forwarded_hop(element) {
                Some(hop) => hops.push(hop),
                None => return HeaderProvenance::Invalid,
            }
        }
    }
    if hops.is_empty() {
        return HeaderProvenance::Invalid;
    }

    boundary_index(
        &hops,
        immediate_peer,
        trusted_proxies,
        |hop: &ForwardedHop| hop.for_address,
    )
    .map(|index| ForwardedBoundary {
        client_address: hops[index].for_address,
        scheme: hops[index].scheme,
    })
    .map_or(HeaderProvenance::Invalid, HeaderProvenance::Valid)
}

fn x_forwarded_for(
    headers: &[(String, String)],
    immediate_peer: IpAddr,
    trusted_proxies: &TrustedProxies,
) -> HeaderProvenance<IpAddr> {
    let values: Vec<&str> = header_values(headers, "x-forwarded-for").collect();
    if values.is_empty() {
        return HeaderProvenance::Absent;
    }

    let mut hops = Vec::new();
    for value in values {
        let elements = match split_quoted(value, ',') {
            Ok(elements) => elements,
            Err(()) => return HeaderProvenance::Invalid,
        };
        for element in elements {
            if hops.len() == MAX_FORWARDED_HOPS {
                return HeaderProvenance::Invalid;
            }
            match parse_forwarded_address(element.trim()) {
                Some(address) => hops.push(address),
                None => return HeaderProvenance::Invalid,
            }
        }
    }
    if hops.is_empty() {
        return HeaderProvenance::Invalid;
    }

    boundary_index(
        &hops,
        immediate_peer,
        trusted_proxies,
        |address: &IpAddr| *address,
    )
    .map(|index| hops[index])
    .map_or(HeaderProvenance::Invalid, HeaderProvenance::Valid)
}

/// Walk a proxy-populated chain from the peer-facing right edge toward the originating client.
///
/// Each trusted predecessor extends the authenticated proxy chain. The first non-trusted
/// predecessor is the client boundary; values farther left could have been supplied by that client
/// and are ignored. If every predecessor is trusted, the oldest retained hop is the farthest
/// authenticated boundary available.
fn boundary_index<T>(
    hops: &[T],
    immediate_peer: IpAddr,
    trusted_proxies: &TrustedProxies,
    address: impl Fn(&T) -> IpAddr,
) -> Option<usize> {
    let mut current_peer = canonical_ip(immediate_peer);
    for index in (0..hops.len()).rev() {
        if !trusted_proxies.contains(current_peer) {
            return None;
        }
        let predecessor = canonical_ip(address(&hops[index]));
        if !trusted_proxies.contains(predecessor) {
            return Some(index);
        }
        current_peer = predecessor;
    }
    (!hops.is_empty()).then_some(0)
}

fn source_headers_agree(
    forwarded: HeaderProvenance<ForwardedBoundary>,
    x_forwarded: HeaderProvenance<IpAddr>,
) -> bool {
    match (forwarded, x_forwarded) {
        (HeaderProvenance::Invalid, _) | (_, HeaderProvenance::Invalid) => false,
        (HeaderProvenance::Valid(boundary), HeaderProvenance::Valid(address)) => {
            boundary.client_address == address
        }
        _ => true,
    }
}

fn x_forwarded_proto(headers: &[(String, String)]) -> HeaderProvenance<EffectiveScheme> {
    let values: Vec<&str> = header_values(headers, "x-forwarded-proto").collect();
    match values.as_slice() {
        [] => HeaderProvenance::Absent,
        [value] if !value.contains(',') => parse_scheme(value.trim())
            .map(HeaderProvenance::Valid)
            .unwrap_or(HeaderProvenance::Invalid),
        // X-Forwarded-Proto has no `for=` identity with which to authenticate a multi-hop list.
        // Require the trusted edge proxy to overwrite it with one value.
        _ => HeaderProvenance::Invalid,
    }
}

fn header_values<'a>(
    headers: &'a [(String, String)],
    name: &'a str,
) -> impl Iterator<Item = &'a str> {
    headers
        .iter()
        .filter(move |(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn parse_forwarded_hop(element: &str) -> Option<ForwardedHop> {
    let params = split_quoted(element, ';').ok()?;
    if params.is_empty() {
        return None;
    }
    let mut seen = HashSet::new();
    let mut for_address = None;
    let mut scheme = None;
    for param in params {
        let (name, raw_value) = param.trim().split_once('=')?;
        let name = name.trim();
        let raw_value = raw_value.trim();
        if !is_token(name) || !seen.insert(name.to_ascii_lowercase()) {
            return None;
        }
        let value = parse_parameter_value(raw_value)?;
        match name.to_ascii_lowercase().as_str() {
            "for" => for_address = Some(parse_forwarded_address(&value)?),
            "proto" => scheme = Some(parse_scheme(&value)?),
            // RFC 7239 `by`, `host`, and registered extensions do not affect this decision, but
            // their syntax was still validated above.
            _ => {}
        }
    }
    Some(ForwardedHop {
        for_address: for_address?,
        scheme,
    })
}

fn parse_forwarded_address(raw: &str) -> Option<IpAddr> {
    if raw.eq_ignore_ascii_case("unknown") || raw.starts_with('_') {
        return None;
    }
    if let Ok(address) = raw.parse::<IpAddr>() {
        let address = canonical_ip(address);
        return (!address.is_unspecified()).then_some(address);
    }
    if let Some(address) = raw.strip_prefix('[').and_then(|v| v.strip_suffix(']')) {
        let address = address.parse::<IpAddr>().ok().map(canonical_ip)?;
        return (!address.is_unspecified()).then_some(address);
    }
    let address = raw
        .parse::<SocketAddr>()
        .ok()
        .map(|address| address.ip())
        .map(canonical_ip)?;
    (!address.is_unspecified()).then_some(address)
}

fn parse_scheme(raw: &str) -> Option<EffectiveScheme> {
    if raw.eq_ignore_ascii_case("https") {
        Some(EffectiveScheme::Https)
    } else if raw.eq_ignore_ascii_case("http") {
        Some(EffectiveScheme::Http)
    } else {
        None
    }
}

fn parse_parameter_value(raw: &str) -> Option<String> {
    if raw.starts_with('"') {
        if raw.len() < 2 || !raw.ends_with('"') {
            return None;
        }
        let mut out = String::with_capacity(raw.len() - 2);
        let mut escaped = false;
        for character in raw[1..raw.len() - 1].chars() {
            if escaped {
                if character.is_ascii_control() {
                    return None;
                }
                out.push(character);
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' || character.is_ascii_control() {
                return None;
            } else {
                out.push(character);
            }
        }
        (!escaped).then_some(out)
    } else {
        is_token(raw).then(|| raw.to_owned())
    }
}

fn is_token(raw: &str) -> bool {
    !raw.is_empty()
        && raw.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn split_quoted(raw: &str, delimiter: char) -> Result<Vec<&str>, ()> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (offset, character) in raw.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            character if character == delimiter && !quoted => {
                let part = raw[start..offset].trim();
                if part.is_empty() {
                    return Err(());
                }
                parts.push(part);
                start = offset + character.len_utf8();
            }
            '\r' | '\n' | '\0' => return Err(()),
            _ => {}
        }
    }
    if quoted || escaped {
        return Err(());
    }
    let tail = raw[start..].trim();
    if tail.is_empty() {
        return Err(());
    }
    parts.push(tail);
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(raw: &str) -> IpAddr {
        raw.parse().unwrap()
    }

    fn headers(values: &[(&str, &str)]) -> Vec<(String, String)> {
        values
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn trusted(raw: &str) -> TrustedProxies {
        TrustedProxies::parse(Some(raw)).unwrap()
    }

    #[test]
    fn trusted_proxy_allowlist_accepts_exact_ips_and_canonical_cidrs() {
        let proxies = trusted("127.0.0.1, 10.20.0.0/16, 2001:db8::/32");
        assert!(proxies.contains(ip("127.0.0.1")));
        assert!(proxies.contains(ip("10.20.4.5")));
        assert!(!proxies.contains(ip("10.21.4.5")));
        assert!(proxies.contains(ip("2001:db8:1::9")));
        assert!(!proxies.contains(ip("2001:db9::1")));
        assert!(TrustedProxies::default().networks.is_empty());
    }

    #[test]
    fn trusted_proxy_allowlist_rejects_ambiguous_or_dangerous_values() {
        for invalid in [
            "",
            "proxy.example",
            "127.0.0.1,",
            "10.20.4.5/16",
            "10.0.0.0/33",
            "0.0.0.0/0",
            "::/0",
            "::ffff:10.0.0.0/120",
            "127.0.0.1,127.0.0.1",
        ] {
            assert!(
                TrustedProxies::parse(Some(invalid)).is_err(),
                "{invalid:?} must be rejected"
            );
        }
    }

    #[test]
    fn direct_tls_is_authoritative_even_with_hostile_forwarded_headers() {
        let result = effective_scheme(
            true,
            ip("203.0.113.10"),
            &headers(&[("forwarded", "garbage"), ("x-forwarded-proto", "http")]),
            &TrustedProxies::default(),
        );
        assert_eq!(result, EffectiveScheme::Https);
    }

    #[test]
    fn untrusted_peer_cannot_assert_https() {
        let result = effective_scheme(
            false,
            ip("203.0.113.10"),
            &headers(&[
                ("forwarded", "for=192.0.2.9;proto=https"),
                ("x-forwarded-proto", "https"),
            ]),
            &trusted("10.0.0.0/8"),
        );
        assert_eq!(result, EffectiveScheme::Http);
    }

    #[test]
    fn untrusted_peer_forwarding_headers_are_ignored_for_client_source() {
        let peer = ip("203.0.113.10");
        let source = client_source(
            peer,
            &headers(&[("forwarded", "not-valid"), ("x-forwarded-for", "192.0.2.1")]),
            &trusted("10.0.0.0/8"),
        );
        assert_eq!(source, ClientSource::Direct(peer));
    }

    #[test]
    fn trusted_edge_accepts_either_valid_client_address_family() {
        let proxies = trusted("10.0.0.9");
        assert_eq!(
            client_source(
                ip("10.0.0.9"),
                &headers(&[("forwarded", "for=203.0.113.7;proto=https")]),
                &proxies,
            ),
            ClientSource::Forwarded(ip("203.0.113.7"))
        );
        assert_eq!(
            client_source(
                ip("10.0.0.9"),
                &headers(&[("x-forwarded-for", "198.51.100.8")]),
                &proxies,
            ),
            ClientSource::Forwarded(ip("198.51.100.8"))
        );
    }

    #[test]
    fn client_chain_walks_right_to_left_and_ignores_untrusted_prefix() {
        let proxies = trusted("10.0.0.0/24");
        let expected = ClientSource::Forwarded(ip("203.0.113.7"));
        assert_eq!(
            client_source(
                ip("10.0.0.3"),
                &headers(&[(
                    "forwarded",
                    "for=198.51.100.8, for=203.0.113.7, for=10.0.0.2",
                )]),
                &proxies,
            ),
            expected
        );
        assert_eq!(
            client_source(
                ip("10.0.0.3"),
                &headers(&[("x-forwarded-for", "198.51.100.8, 203.0.113.7, 10.0.0.2",)]),
                &proxies,
            ),
            expected
        );
    }

    #[test]
    fn mapped_ipv6_client_addresses_are_canonicalized_before_agreement() {
        let proxies = trusted("10.0.0.9");
        assert_eq!(
            client_source(
                ip("10.0.0.9"),
                &headers(&[
                    ("forwarded", r#"for="[::ffff:192.0.2.7]:443""#),
                    ("x-forwarded-for", "::ffff:192.0.2.7"),
                ]),
                &proxies,
            ),
            ClientSource::Forwarded(ip("192.0.2.7"))
        );
    }

    #[test]
    fn trusted_peer_without_usable_provenance_never_becomes_the_client() {
        let proxies = trusted("10.0.0.9");
        for values in [
            vec![],
            vec![("forwarded", "for=unknown;proto=https")],
            vec![("x-forwarded-for", "unknown")],
            vec![("x-forwarded-for", "0.0.0.0")],
            vec![
                ("forwarded", "for=203.0.113.7;proto=https"),
                ("x-forwarded-for", "not-an-address"),
            ],
        ] {
            assert_eq!(
                client_source(ip("10.0.0.9"), &headers(&values), &proxies),
                ClientSource::Unavailable,
                "{values:?} must not fall back to the trusted proxy address"
            );
        }
    }

    #[test]
    fn forwarded_and_xff_must_resolve_to_the_same_client() {
        let proxies = trusted("10.0.0.9");
        let agreeing = headers(&[
            ("forwarded", "for=203.0.113.7;proto=https"),
            ("x-forwarded-for", "203.0.113.7"),
            ("x-forwarded-proto", "https"),
        ]);
        assert_eq!(
            client_source(ip("10.0.0.9"), &agreeing, &proxies),
            ClientSource::Forwarded(ip("203.0.113.7"))
        );
        assert_eq!(
            effective_scheme(false, ip("10.0.0.9"), &agreeing, &proxies),
            EffectiveScheme::Https
        );

        let conflicting = headers(&[
            ("forwarded", "for=203.0.113.7;proto=https"),
            ("x-forwarded-for", "198.51.100.8"),
            ("x-forwarded-proto", "https"),
        ]);
        assert_eq!(
            client_source(ip("10.0.0.9"), &conflicting, &proxies),
            ClientSource::Unavailable
        );
        assert_eq!(
            effective_scheme(false, ip("10.0.0.9"), &conflicting, &proxies),
            EffectiveScheme::Http
        );
    }

    #[test]
    fn trusted_edge_proxy_can_assert_https_with_either_header_family() {
        let proxies = trusted("10.0.0.9");
        assert_eq!(
            effective_scheme(
                false,
                ip("10.0.0.9"),
                &headers(&[("forwarded", "for=203.0.113.7;proto=https")]),
                &proxies,
            ),
            EffectiveScheme::Https
        );
        assert_eq!(
            effective_scheme(
                false,
                ip("10.0.0.9"),
                &headers(&[("x-forwarded-proto", "HTTPS")]),
                &proxies,
            ),
            EffectiveScheme::Https
        );
    }

    #[test]
    fn validated_multi_hop_chain_uses_the_first_untrusted_boundary() {
        let proxies = trusted("10.0.0.0/24");
        let result = effective_scheme(
            false,
            ip("10.0.0.3"),
            &headers(&[(
                "forwarded",
                "for=203.0.113.7;proto=https, for=10.0.0.2;proto=http",
            )]),
            &proxies,
        );
        assert_eq!(result, EffectiveScheme::Https);

        // The leftmost value is attacker-controlled once the rightmost hop names an untrusted
        // predecessor. It cannot override that authenticated boundary's HTTP scheme.
        let injected = effective_scheme(
            false,
            ip("10.0.0.3"),
            &headers(&[(
                "forwarded",
                "for=198.51.100.8;proto=https, for=203.0.113.7;proto=http",
            )]),
            &proxies,
        );
        assert_eq!(injected, EffectiveScheme::Http);
    }

    #[test]
    fn malformed_duplicate_or_conflicting_provenance_fails_closed() {
        let proxies = trusted("10.0.0.9");
        for values in [
            vec![("forwarded", "for=203.0.113.7")],
            vec![("forwarded", "for=unknown;proto=https")],
            vec![("forwarded", "for=203.0.113.7;proto=https;proto=https")],
            vec![("x-forwarded-proto", "https,http")],
            vec![
                ("forwarded", "for=203.0.113.7;proto=https"),
                ("x-forwarded-proto", "http"),
            ],
            vec![
                ("forwarded", "for=203.0.113.7;proto=https"),
                ("x-forwarded-proto", "https"),
                ("x-forwarded-proto", "https"),
            ],
        ] {
            assert_eq!(
                effective_scheme(false, ip("10.0.0.9"), &headers(&values), &proxies),
                EffectiveScheme::Http,
                "{values:?} must fail closed"
            );
        }
    }

    #[test]
    fn quoted_ipv6_forwarded_node_is_validated() {
        let result = effective_scheme(
            false,
            ip("2001:db8::9"),
            &headers(&[("forwarded", r#"for="[2001:db9::7]:443";proto="https""#)]),
            &trusted("2001:db8::/32"),
        );
        assert_eq!(result, EffectiveScheme::Https);
    }
}
