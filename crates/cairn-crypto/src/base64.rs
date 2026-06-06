//! A tiny, dependency-free standard-alphabet base64 codec, used only for the master-key
//! `from_base64` constructor convenience. The workspace deliberately does not vendor a
//! base64 crate, and this codec is small, total, and has no `unsafe`.
//!
//! Standard RFC 4648 alphabet (`A–Z a–z 0–9 + /`). Encoding emits `=` padding; decoding
//! accepts input with or without padding but rejects any other stray character.

/// An invalid base64 input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeError;

/// Encode bytes to a standard, padded base64 string. Only the decode path is needed in
/// production (a master key is supplied encoded and decoded once); `encode` exists for the
/// round-trip tests and key-generation tooling, so it is compiled only under `cfg(test)`.
#[cfg(test)]
const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode bytes to a standard, padded base64 string.
#[cfg(test)]
#[must_use]
pub fn encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;

        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Map an alphabet byte back to its 6-bit value, or `None` if it is not in the alphabet.
fn unmap(b: u8) -> Option<u32> {
    match b {
        b'A'..=b'Z' => Some(u32::from(b - b'A')),
        b'a'..=b'z' => Some(u32::from(b - b'a') + 26),
        b'0'..=b'9' => Some(u32::from(b - b'0') + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// Decode a standard base64 string, accepting input with or without `=` padding.
///
/// # Errors
/// Returns [`DecodeError`] if the input contains characters outside the standard alphabet
/// (other than trailing `=` padding) or has an invalid length.
pub fn decode(input: &str) -> Result<Vec<u8>, DecodeError> {
    // Strip trailing padding; reject any '=' that is not trailing.
    let bytes = input.as_bytes();
    let unpadded_len = bytes.iter().take_while(|&&b| b != b'=').count();
    if bytes[unpadded_len..].iter().any(|&b| b != b'=') {
        return Err(DecodeError);
    }
    let sym = &bytes[..unpadded_len];

    // A base64 group is 4 symbols -> 3 bytes; a final partial group of 2 or 3 symbols is
    // valid (1 or 2 output bytes), but a remainder of exactly 1 symbol is not.
    if sym.len() % 4 == 1 {
        return Err(DecodeError);
    }

    let mut out = Vec::with_capacity(sym.len() / 4 * 3 + 2);
    let mut acc = 0u32;
    let mut bits = 0u32;
    for &b in sym {
        let v = unmap(b).ok_or(DecodeError)?;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xff) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_various_lengths() {
        for len in 0..=64usize {
            let data: Vec<u8> = (0..len).map(|i| (i * 7 + 3) as u8).collect();
            let encoded = encode(&data);
            let decoded = decode(&encoded).expect("decode");
            assert_eq!(decoded, data, "round-trip failed at len {len}");
        }
    }

    #[test]
    fn known_vectors() {
        assert_eq!(encode(b""), "");
        assert_eq!(encode(b"f"), "Zg==");
        assert_eq!(encode(b"fo"), "Zm8=");
        assert_eq!(encode(b"foo"), "Zm9v");
        assert_eq!(encode(b"foob"), "Zm9vYg==");
        assert_eq!(encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(encode(b"foobar"), "Zm9vYmFy");

        assert_eq!(decode("Zg==").unwrap(), b"f");
        assert_eq!(decode("Zm9vYmFy").unwrap(), b"foobar");
    }

    #[test]
    fn decodes_without_padding() {
        assert_eq!(decode("Zg").unwrap(), b"f");
        assert_eq!(decode("Zm8").unwrap(), b"fo");
        assert_eq!(decode("Zm9vYg").unwrap(), b"foob");
    }

    #[test]
    fn rejects_stray_characters() {
        assert_eq!(decode("Zm9v!"), Err(DecodeError));
        assert_eq!(decode("Z=g="), Err(DecodeError));
        assert_eq!(decode("====").unwrap(), b""); // all padding decodes to empty
    }

    #[test]
    fn rejects_lone_trailing_symbol() {
        // A single dangling 6-bit symbol cannot form any output byte.
        assert_eq!(decode("Z"), Err(DecodeError));
        assert_eq!(decode("Zm9vZ"), Err(DecodeError));
    }

    #[test]
    fn thirty_two_byte_key_round_trips() {
        let key = [0xA5u8; 32];
        let encoded = encode(&key);
        assert_eq!(decode(&encoded).unwrap(), key);
    }
}
