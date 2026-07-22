//! `cairn-protocol` — the S3 protocol layer: request dispatch, the request lifecycles, the streaming
//! chunked-upload decoder, and the total error translator to S3 XML. Handlers depend only on
//! the trait spine in `cairn-types`.

#![forbid(unsafe_code)]

pub mod chunked;
pub mod error_map;
mod httpdate;
pub mod keyprovider;
pub mod request;
pub mod service;

pub use chunked::{ChunkDecoder, ChunkVerifier, DecodeError};
pub use error_map::error_response;
pub use keyprovider::{KeyProvider, LocalRingProvider};
pub use request::{S3Body, S3Request, S3Response};
pub use service::S3Service;
