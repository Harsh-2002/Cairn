//! The single, total translator from the canonical [`Error`] to an S3 XML error response
//! (ARCH §25). The match has no wildcard arm, so the compiler guarantees every error variant
//! maps to a defined HTTP status and S3 error code.

use crate::request::S3Response;
use cairn_types::error::Error;
use http::StatusCode;

/// Map an error to (HTTP status, S3 error code).
#[must_use]
pub fn map(err: &Error) -> (StatusCode, &'static str) {
    use Error::*;
    match err {
        NoSuchBucket => (StatusCode::NOT_FOUND, "NoSuchBucket"),
        NoSuchKey => (StatusCode::NOT_FOUND, "NoSuchKey"),
        NoSuchVersion => (StatusCode::NOT_FOUND, "NoSuchVersion"),
        BucketAlreadyExists => (StatusCode::CONFLICT, "BucketAlreadyExists"),
        BucketAlreadyOwnedByYou => (StatusCode::CONFLICT, "BucketAlreadyOwnedByYou"),
        BucketNotEmpty => (StatusCode::CONFLICT, "BucketNotEmpty"),
        NoSuchUpload => (StatusCode::NOT_FOUND, "NoSuchUpload"),
        PreconditionFailed => (StatusCode::PRECONDITION_FAILED, "PreconditionFailed"),
        EntityTooLarge => (StatusCode::BAD_REQUEST, "EntityTooLarge"),
        InsufficientStorage => (StatusCode::INSUFFICIENT_STORAGE, "InsufficientStorage"),
        BadDigest => (StatusCode::BAD_REQUEST, "BadDigest"),
        InvalidDigest => (StatusCode::BAD_REQUEST, "InvalidDigest"),
        MalformedXml => (StatusCode::BAD_REQUEST, "MalformedXML"),
        InvalidTag(_) => (StatusCode::BAD_REQUEST, "InvalidTag"),
        MalformedPolicy => (StatusCode::BAD_REQUEST, "MalformedPolicy"),
        InvalidArgument(_) => (StatusCode::BAD_REQUEST, "InvalidArgument"),
        InvalidRequest(_) => (StatusCode::BAD_REQUEST, "InvalidRequest"),
        AccessDenied => (StatusCode::FORBIDDEN, "AccessDenied"),
        InvalidAccessKeyId => (StatusCode::FORBIDDEN, "InvalidAccessKeyId"),
        SignatureDoesNotMatch => (StatusCode::FORBIDDEN, "SignatureDoesNotMatch"),
        InvalidRange => (StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange"),
        NotImplemented => (StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
        AclNotSupported => (StatusCode::BAD_REQUEST, "AccessControlListNotSupported"),
        Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
    }
}

/// Render an error to an S3 XML error response with the request id echoed.
#[must_use]
pub fn error_response(err: &Error, resource: &str, request_id: &str) -> S3Response {
    let (status, code) = map(err);
    // Never surface internal error detail (crypto/blob/IO/internal messages) in the client-facing
    // `<Message>` (audit #28): a 5xx logs the real cause server-side and returns a generic message,
    // matching AWS's opaque InternalError. Client (4xx) errors keep their descriptive, S3-standard
    // message.
    let message = if status.is_server_error() {
        tracing::error!(error = %err, request_id, resource, "internal error serving request");
        "We encountered an internal error. Please try again.".to_owned()
    } else {
        err.to_string()
    };
    let body = cairn_xml::error_document(code, &message, resource, request_id);
    S3Response::xml(status, body).with_header("x-amz-request-id", request_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn representative_mappings() {
        assert_eq!(map(&Error::NoSuchBucket).0, StatusCode::NOT_FOUND);
        assert_eq!(
            map(&Error::PreconditionFailed).0,
            StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            map(&Error::InsufficientStorage).0,
            StatusCode::INSUFFICIENT_STORAGE
        );
        assert_eq!(
            map(&Error::InvalidRange).0,
            StatusCode::RANGE_NOT_SATISFIABLE
        );
        assert_eq!(map(&Error::AccessDenied).1, "AccessDenied");
        assert_eq!(
            map(&Error::Internal("x".into())).0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn error_document_includes_code_and_request_id() {
        let r = error_response(&Error::NoSuchKey, "/b/k", "req-123");
        assert_eq!(r.status, StatusCode::NOT_FOUND);
        let crate::request::S3Body::Bytes(b) = r.body else {
            panic!("expected bytes body")
        };
        let s = String::from_utf8(b.to_vec()).unwrap();
        assert!(s.contains("NoSuchKey"));
        assert!(s.contains("req-123"));
    }

    #[test]
    fn internal_error_detail_is_not_leaked() {
        // Audit #28: a 5xx must not echo the internal error detail in the client-facing message.
        let r = error_response(
            &Error::Internal("sekret crypto/blob/io detail".to_owned()),
            "/b/k",
            "req-9",
        );
        assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR);
        let crate::request::S3Body::Bytes(b) = r.body else {
            panic!("expected bytes body")
        };
        let s = String::from_utf8(b.to_vec()).unwrap();
        assert!(!s.contains("sekret"), "internal detail leaked: {s}");
        assert!(s.contains("InternalError"), "keeps the generic S3 code");
        assert!(s.contains("req-9"));
    }
}
