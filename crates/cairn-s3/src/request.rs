//! A library-neutral S3 request/response representation, so the protocol handlers are testable
//! without an HTTP server. `cairn-server` adapts hyper to these; tests construct them directly.

use bytes::Bytes;
use cairn_types::auth::Principal;
use cairn_types::id::{BucketName, ObjectKey};
use http::{Method, StatusCode};
use std::net::IpAddr;

/// An incoming, already-routed S3 request.
pub struct S3Request {
    /// The HTTP method.
    pub method: Method,
    /// The target bucket (None for service-level operations like ListBuckets).
    pub bucket: Option<BucketName>,
    /// The target object key (None for bucket/service-level operations).
    pub key: Option<ObjectKey>,
    /// Decoded query parameters.
    pub query: Vec<(String, String)>,
    /// Request headers (lowercased names).
    pub headers: Vec<(String, String)>,
    /// The authenticated principal, or None for anonymous.
    pub principal: Option<Principal>,
    /// Source address.
    pub source: IpAddr,
    /// Whether the transport was secure.
    pub secure: bool,
    /// A correlation id echoed in the response and error documents.
    pub request_id: String,
}

impl S3Request {
    /// The first value of a header (lowercased `name`).
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The first value of a query parameter.
    #[must_use]
    pub fn query(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Whether a (possibly valueless) query key is present (subresource marker).
    #[must_use]
    pub fn has_query(&self, name: &str) -> bool {
        self.query.iter().any(|(k, _)| k == name)
    }
}

impl std::fmt::Debug for S3Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Request")
            .field("method", &self.method)
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

/// A response body: empty, an in-memory buffer (XML/errors), or a streamed blob.
pub enum S3Body {
    /// No body.
    Empty,
    /// A fully-buffered body (XML documents, error responses).
    Bytes(Bytes),
    /// A streamed body (object reads), with its content length.
    Stream {
        /// The length of the stream in bytes.
        length: u64,
        /// The byte stream.
        stream: cairn_types::BlobStream,
    },
}

impl std::fmt::Debug for S3Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Body::Empty => f.write_str("Empty"),
            S3Body::Bytes(b) => f.debug_tuple("Bytes").field(&b.len()).finish(),
            S3Body::Stream { length, .. } => f
                .debug_struct("Stream")
                .field("length", length)
                .finish_non_exhaustive(),
        }
    }
}

/// An outgoing S3 response.
pub struct S3Response {
    /// HTTP status.
    pub status: StatusCode,
    /// Response headers.
    pub headers: Vec<(String, String)>,
    /// Response body.
    pub body: S3Body,
}

impl S3Response {
    /// A response with a status and no body.
    #[must_use]
    pub fn status(status: StatusCode) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: S3Body::Empty,
        }
    }

    /// An XML response.
    #[must_use]
    pub fn xml(status: StatusCode, body: String) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_owned(), "application/xml".to_owned())],
            body: S3Body::Bytes(Bytes::from(body)),
        }
    }

    /// Add a header.
    #[must_use]
    pub fn with_header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_owned(), value.into()));
        self
    }
}

impl std::fmt::Debug for S3Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Response")
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}
