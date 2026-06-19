#![no_main]
//! Fuzz the bucket-policy JSON parser (ARCH 29.3, 15.5; GAPS Medium #11). `parse_policy`
//! turns an attacker-controllable policy document into a typed `Policy`, folding every
//! structural problem — bad JSON, missing fields, wrong shapes, unknown effect/operator — into
//! `Error::MalformedPolicy`. It must never panic on arbitrary input.

use cairn_authz::parse_policy;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // `parse_policy` takes `&str`; non-UTF-8 input is simply out of its domain, so we hand it
    // the longest valid UTF-8 prefix. (`from_utf8` here would discard most of the corpus.)
    let s = match std::str::from_utf8(data) {
        Ok(s) => s,
        Err(e) => match std::str::from_utf8(&data[..e.valid_up_to()]) {
            Ok(s) => s,
            Err(_) => return,
        },
    };
    // The only property under test is the absence of a panic; the typed result is discarded.
    let _ = parse_policy(s);
});
