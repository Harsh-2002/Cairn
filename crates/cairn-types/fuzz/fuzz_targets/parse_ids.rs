#![no_main]
//! Fuzz the name/key sanitizers (ARCH 29.3, audit #31/#32). `ObjectKey::parse` and
//! `BucketName::parse` validate attacker-controlled identifiers (length, charset, XML-illegal
//! control bytes, dot/structural edge cases) and must fold every bad input to a typed
//! `InvalidName` — never a panic. We feed the longest valid-UTF-8 prefix of the arbitrary bytes
//! (these parsers take `&str`; non-UTF-8 is out of their domain).

use cairn_types::id::{BucketName, ObjectKey};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let s = match std::str::from_utf8(data) {
        Ok(s) => s,
        Err(e) => match std::str::from_utf8(&data[..e.valid_up_to()]) {
            Ok(s) => s,
            Err(_) => return,
        },
    };
    // The only property under test is the absence of an unwind; the typed result is discarded.
    let _ = ObjectKey::parse(s);
    let _ = BucketName::parse(s);
});
