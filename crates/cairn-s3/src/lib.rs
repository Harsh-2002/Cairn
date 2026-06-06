//! `cairn-s3` — the S3 protocol layer: request dispatch, the seven request lifecycles, the
//! streaming chunked-upload decoder, and the total error translator to S3 XML. Handlers depend
//! only on the trait spine in `cairn-types`.

#![forbid(unsafe_code)]

pub mod chunked;

pub use chunked::{ChunkDecoder, ChunkVerifier, DecodeError};
