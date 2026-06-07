//! Hashing, HMAC, AWS URI-encoding, and timestamp helpers shared by the SigV4 code.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// SHA-256 of `data`, lowercase hex.
#[must_use]
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// HMAC-SHA256(key, data) raw bytes.
#[must_use]
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// AWS URI-encoding (RFC 3986 unreserved kept; everything else percent-encoded). When
/// `encode_slash` is false, `/` is preserved (used for canonical URI paths).
#[must_use]
pub fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b'/' if !encode_slash => out.push('/'),
            _ => {
                out.push('%');
                out.push(hex_upper(b >> 4));
                out.push(hex_upper(b & 0xf));
            }
        }
    }
    out
}

/// Percent-decode `%XX` escapes (leaving `+` literal, per SigV4 query semantics).
#[must_use]
pub fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex_val(b[i + 1]), hex_val(b[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// Parse an `amz-date` (`YYYYMMDDTHHMMSSZ`) to milliseconds since the Unix epoch.
#[must_use]
pub fn parse_amz_date(s: &str) -> Option<i64> {
    // Format: 8 date digits, 'T', 6 time digits, 'Z' (16 chars).
    let b = s.as_bytes();
    if b.len() != 16 || b[8] != b'T' || b[15] != b'Z' {
        return None;
    }
    let num = |range: std::ops::Range<usize>| -> Option<i64> { s.get(range)?.parse::<i64>().ok() };
    let year = num(0..4)?;
    let month = num(4..6)?;
    let day = num(6..8)?;
    let hour = num(9..11)?;
    let min = num(11..13)?;
    let sec = num(13..15)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    let days = days_from_civil(year, month as u32, day as u32);
    Some((days * 86400 + hour * 3600 + min * 60 + sec) * 1000)
}

/// Days from the Unix epoch for a civil (proleptic Gregorian) date (Hinnant's algorithm).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_empty() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn uri_encoding_rules() {
        assert_eq!(uri_encode("a b/c", false), "a%20b/c");
        assert_eq!(uri_encode("a b/c", true), "a%20b%2Fc");
        assert_eq!(uri_encode("-_.~", false), "-_.~");
    }

    #[test]
    fn canonical_uri_encodes_the_wire_path_exactly_once() {
        // The request path arrives already percent-encoded; the SigV4 canonical URI must
        // decode-then-encode-once so it matches what the client signed — never double-encode
        // (the bug that turned %28 into %2528 and broke keys with '(' ')' or spaces).
        for wire in ["/bkt/a%281%29.rnd", "/bkt/with%20space.txt", "/bkt/p%29%28q"] {
            assert_eq!(uri_encode(&percent_decode(wire), false), wire);
        }
    }

    #[test]
    fn amz_date_parsing() {
        // 2015-08-30T12:36:00Z
        let ms = parse_amz_date("20150830T123600Z").unwrap();
        assert_eq!(ms, 1_440_938_160_000);
        assert!(parse_amz_date("bogus").is_none());
        assert!(parse_amz_date("20150830T123600X").is_none());
    }
}
