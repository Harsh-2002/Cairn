#![no_main]
//! Fuzz the S3 request-body XML parsers (ARCH 29.3, GAPS Medium #11). Every public parser
//! entry point folds malformed input — invalid UTF-8, unbalanced tags, missing required
//! fields, out-of-range numbers — to a typed `Error::MalformedXml`; none may ever panic. We
//! feed the same arbitrary bytes through each parser so libfuzzer can drive all of them with a
//! shared corpus, and assert only that they return (Ok or Err) without unwinding.

use cairn_xml::{
    parse_complete_multipart, parse_cors_configuration, parse_delete, parse_tagging,
    parse_versioning_configuration,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|body: &[u8]| {
    // The contract is total parsing: a typed error for bad input, never a panic. Discard the
    // results; the only property under test is the absence of an unwind.
    let _ = parse_complete_multipart(body);
    let _ = parse_delete(body);
    let _ = parse_tagging(body);
    let _ = parse_versioning_configuration(body);
    let _ = parse_cors_configuration(body);
});
