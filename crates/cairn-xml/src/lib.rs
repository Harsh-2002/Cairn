//! `cairn-xml` — the S3-compatible XML request/response codec (ARCH §13.4, §21.4).
//!
//! This crate is the single place where Cairn translates its domain types to and from the
//! XML wire shapes S3 clients expect. Generators return owned `String`s (UTF-8, no BOM,
//! each prefixed with the `<?xml ... ?>` declaration); parsers take `&[u8]` request bodies
//! and yield [`cairn_types::Error`], folding every malformed input to [`MalformedXml`]
//! so the protocol layer's error translator stays total.
//!
//! ETags are rendered quoted (the one quoting point S3 requires); all character data is
//! escaped through quick-xml. Timestamps render as ISO-8601 UTC with millisecond precision
//! via a small hand-rolled formatter (no `chrono`).
//!
//! [`MalformedXml`]: cairn_types::Error::MalformedXml

#![forbid(unsafe_code)]

use cairn_types::{
    Bucket, ETag, ListPage, MultipartSession, ObjectSummary, PartRecord, StorageClass, Timestamp,
    VersioningState,
};
use quick_xml::Writer;
use quick_xml::events::{BytesDecl, BytesText, Event};
use std::io::Cursor;

mod parse;
mod timefmt;

pub use parse::{CorsRule, parse_complete_multipart, parse_cors_configuration, parse_delete};
pub use parse::{parse_tagging, parse_versioning_configuration};
pub use timefmt::format_iso8601;

// ===========================================================================================
// Small writer helpers
// ===========================================================================================

/// A `Writer` over an in-memory buffer, primed with the XML declaration.
fn new_doc() -> Writer<Cursor<Vec<u8>>> {
    let mut w = Writer::new(Cursor::new(Vec::new()));
    // `<?xml version="1.0" encoding="UTF-8"?>`. quick-xml writes no trailing newline.
    w.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
        .expect("writing to an in-memory buffer is infallible");
    w
}

/// Finish a document, returning its UTF-8 string. The buffer only ever contains bytes we
/// produced from `&str`s, so it is valid UTF-8 by construction.
fn finish(w: Writer<Cursor<Vec<u8>>>) -> String {
    let bytes = w.into_inner().into_inner();
    String::from_utf8(bytes).expect("generated XML is valid UTF-8 by construction")
}

/// Write a leaf element `<name>escaped-text</name>`.
fn leaf(w: &mut Writer<Cursor<Vec<u8>>>, name: &str, text: &str) {
    w.create_element(name)
        .write_text_content(BytesText::new(text))
        .expect("writing to an in-memory buffer is infallible");
}

/// Write a quoted-ETag leaf: `<name>"value"</name>` (quotes are part of the text content,
/// escaped along with the rest).
fn etag_leaf(w: &mut Writer<Cursor<Vec<u8>>>, name: &str, etag: &ETag) {
    let quoted = format!("\"{}\"", etag.as_str());
    leaf(w, name, &quoted);
}

/// Render a [`StorageClass`] to its S3 token.
fn storage_class_str(sc: StorageClass) -> &'static str {
    match sc {
        StorageClass::Standard => "STANDARD",
        StorageClass::ColdTier => "GLACIER",
    }
}

/// Write a standard `<Owner><ID/><DisplayName/></Owner>` block.
fn owner(w: &mut Writer<Cursor<Vec<u8>>>, id: &str, display: &str) {
    w.create_element("Owner")
        .write_inner_content(|w| {
            leaf(w, "ID", id);
            leaf(w, "DisplayName", display);
            Ok(())
        })
        .expect("writing to an in-memory buffer is infallible");
}

// ===========================================================================================
// Error document
// ===========================================================================================

/// The S3 `<Error>` document carrying the error code, human message, the resource path, and
/// the request id (which also appears as a response header and trace span, ARCH §13.4).
#[must_use]
pub fn error_document(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    let mut w = new_doc();
    w.create_element("Error")
        .write_inner_content(|w| {
            leaf(w, "Code", code);
            leaf(w, "Message", message);
            leaf(w, "Resource", resource);
            leaf(w, "RequestId", request_id);
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

// ===========================================================================================
// Object listing — ListObjectsV2 / V1 / Versions
// ===========================================================================================

/// `ListBucketResult` in the V2 (continuation-token) form.
#[must_use]
pub fn list_objects_v2(
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    max_keys: u32,
    page: &ListPage<ObjectSummary>,
    continuation_token: Option<&str>,
) -> String {
    let mut w = new_doc();
    w.create_element("ListBucketResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            leaf(w, "Name", bucket);
            leaf(w, "Prefix", prefix.unwrap_or(""));
            if let Some(d) = delimiter {
                leaf(w, "Delimiter", d);
            }
            leaf(w, "MaxKeys", &max_keys.to_string());
            // KeyCount is the number of Contents + CommonPrefixes returned.
            let key_count = page.items.len() + page.common_prefixes.len();
            leaf(w, "KeyCount", &key_count.to_string());
            leaf(
                w,
                "IsTruncated",
                if page.truncated { "true" } else { "false" },
            );
            if let Some(ct) = continuation_token {
                leaf(w, "ContinuationToken", ct);
            }
            if let Some(next) = &page.next_cursor {
                leaf(w, "NextContinuationToken", next);
            }
            for item in &page.items {
                write_contents(w, item);
            }
            write_common_prefixes(w, &page.common_prefixes);
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

/// `ListBucketResult` in the V1 (marker) form.
#[must_use]
pub fn list_objects_v1(
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    max_keys: u32,
    page: &ListPage<ObjectSummary>,
    marker: Option<&str>,
) -> String {
    let mut w = new_doc();
    w.create_element("ListBucketResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            leaf(w, "Name", bucket);
            leaf(w, "Prefix", prefix.unwrap_or(""));
            leaf(w, "Marker", marker.unwrap_or(""));
            if let Some(d) = delimiter {
                leaf(w, "Delimiter", d);
            }
            leaf(w, "MaxKeys", &max_keys.to_string());
            leaf(
                w,
                "IsTruncated",
                if page.truncated { "true" } else { "false" },
            );
            // In the V1 form, NextMarker is only meaningful when a delimiter is present, but
            // S3 emits it whenever the result is truncated; mirror that.
            if page.truncated {
                if let Some(next) = &page.next_cursor {
                    leaf(w, "NextMarker", next);
                }
            }
            for item in &page.items {
                write_contents(w, item);
            }
            write_common_prefixes(w, &page.common_prefixes);
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

/// `ListVersionsResult`, distinguishing `Version` from `DeleteMarker` entries.
#[must_use]
pub fn list_object_versions(
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    max_keys: u32,
    page: &ListPage<ObjectSummary>,
    key_marker: Option<&str>,
    version_id_marker: Option<&str>,
) -> String {
    let mut w = new_doc();
    w.create_element("ListVersionsResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            leaf(w, "Name", bucket);
            leaf(w, "Prefix", prefix.unwrap_or(""));
            leaf(w, "KeyMarker", key_marker.unwrap_or(""));
            leaf(w, "VersionIdMarker", version_id_marker.unwrap_or(""));
            if let Some(d) = delimiter {
                leaf(w, "Delimiter", d);
            }
            leaf(w, "MaxKeys", &max_keys.to_string());
            leaf(
                w,
                "IsTruncated",
                if page.truncated { "true" } else { "false" },
            );
            if page.truncated {
                if let Some(next) = &page.next_cursor {
                    leaf(w, "NextKeyMarker", next);
                }
            }
            for item in &page.items {
                write_version_entry(w, item);
            }
            write_common_prefixes(w, &page.common_prefixes);
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

/// `<Contents>` entry for a plain object listing.
fn write_contents(w: &mut Writer<Cursor<Vec<u8>>>, item: &ObjectSummary) {
    let owner_id = item.owner_id.to_string();
    w.create_element("Contents")
        .write_inner_content(|w| {
            leaf(w, "Key", item.key.as_str());
            leaf(w, "LastModified", &format_iso8601(item.last_modified));
            etag_leaf(w, "ETag", &item.etag);
            leaf(w, "Size", &item.size.to_string());
            leaf(w, "StorageClass", storage_class_str(item.storage_class));
            owner(w, &owner_id, &owner_id);
            Ok(())
        })
        .expect("infallible");
}

/// A `<Version>` or `<DeleteMarker>` entry for a versions listing.
fn write_version_entry(w: &mut Writer<Cursor<Vec<u8>>>, item: &ObjectSummary) {
    let owner_id = item.owner_id.to_string();
    let tag = if item.is_delete_marker {
        "DeleteMarker"
    } else {
        "Version"
    };
    let is_delete_marker = item.is_delete_marker;
    let etag = item.etag.clone();
    let storage_class = item.storage_class;
    let size = item.size;
    w.create_element(tag)
        .write_inner_content(|w| {
            leaf(w, "Key", item.key.as_str());
            leaf(w, "VersionId", item.version_id.as_str());
            leaf(w, "IsLatest", if item.is_latest { "true" } else { "false" });
            leaf(w, "LastModified", &format_iso8601(item.last_modified));
            if !is_delete_marker {
                etag_leaf(w, "ETag", &etag);
                leaf(w, "Size", &size.to_string());
                leaf(w, "StorageClass", storage_class_str(storage_class));
            }
            owner(w, &owner_id, &owner_id);
            Ok(())
        })
        .expect("infallible");
}

/// `<CommonPrefixes><Prefix/></CommonPrefixes>` for each grouped prefix.
fn write_common_prefixes(w: &mut Writer<Cursor<Vec<u8>>>, prefixes: &[String]) {
    for p in prefixes {
        w.create_element("CommonPrefixes")
            .write_inner_content(|w| {
                leaf(w, "Prefix", p);
                Ok(())
            })
            .expect("infallible");
    }
}

// ===========================================================================================
// ListAllMyBuckets
// ===========================================================================================

/// `ListAllMyBucketsResult` — the service-level bucket enumeration.
#[must_use]
pub fn list_buckets(owner_id: &str, owner_display: &str, buckets: &[Bucket]) -> String {
    let mut w = new_doc();
    w.create_element("ListAllMyBucketsResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            owner(w, owner_id, owner_display);
            w.create_element("Buckets").write_inner_content(|w| {
                for b in buckets {
                    w.create_element("Bucket").write_inner_content(|w| {
                        leaf(w, "Name", b.name.as_str());
                        leaf(w, "CreationDate", &format_iso8601(b.created_at));
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

// ===========================================================================================
// Multipart
// ===========================================================================================

/// `InitiateMultipartUploadResult`.
#[must_use]
pub fn initiate_multipart_result(bucket: &str, key: &str, upload_id: &str) -> String {
    let mut w = new_doc();
    w.create_element("InitiateMultipartUploadResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            leaf(w, "Bucket", bucket);
            leaf(w, "Key", key);
            leaf(w, "UploadId", upload_id);
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

/// `CompleteMultipartUploadResult` (the ETag is rendered quoted).
#[must_use]
pub fn complete_multipart_result(location: &str, bucket: &str, key: &str, etag: &ETag) -> String {
    let mut w = new_doc();
    w.create_element("CompleteMultipartUploadResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            leaf(w, "Location", location);
            leaf(w, "Bucket", bucket);
            leaf(w, "Key", key);
            etag_leaf(w, "ETag", etag);
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

/// `ListPartsResult`.
#[must_use]
pub fn list_parts_result(
    bucket: &str,
    key: &str,
    upload_id: &str,
    page: &ListPage<PartRecord>,
    owner_id: &str,
    part_number_marker: u16,
    max_parts: u32,
) -> String {
    let mut w = new_doc();
    w.create_element("ListPartsResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            leaf(w, "Bucket", bucket);
            leaf(w, "Key", key);
            leaf(w, "UploadId", upload_id);
            owner(w, owner_id, owner_id);
            leaf(w, "StorageClass", "STANDARD");
            leaf(w, "PartNumberMarker", &part_number_marker.to_string());
            if let Some(next) = &page.next_cursor {
                leaf(w, "NextPartNumberMarker", next);
            }
            leaf(w, "MaxParts", &max_parts.to_string());
            leaf(
                w,
                "IsTruncated",
                if page.truncated { "true" } else { "false" },
            );
            for part in &page.items {
                w.create_element("Part").write_inner_content(|w| {
                    leaf(w, "PartNumber", &part.part_number.to_string());
                    // The part ETag is stored as bare hex; render it quoted.
                    leaf(w, "ETag", &format!("\"{}\"", part.etag));
                    leaf(w, "Size", &part.size.to_string());
                    Ok(())
                })?;
            }
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

/// `ListMultipartUploadsResult`.
#[must_use]
pub fn list_multipart_uploads_result(
    bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    page: &ListPage<MultipartSession>,
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
    max_uploads: u32,
) -> String {
    let mut w = new_doc();
    w.create_element("ListMultipartUploadsResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            leaf(w, "Bucket", bucket);
            leaf(w, "KeyMarker", key_marker.unwrap_or(""));
            leaf(w, "UploadIdMarker", upload_id_marker.unwrap_or(""));
            if page.truncated {
                if let Some(next) = &page.next_cursor {
                    leaf(w, "NextKeyMarker", next);
                }
            }
            if let Some(p) = prefix {
                leaf(w, "Prefix", p);
            }
            if let Some(d) = delimiter {
                leaf(w, "Delimiter", d);
            }
            leaf(w, "MaxUploads", &max_uploads.to_string());
            leaf(
                w,
                "IsTruncated",
                if page.truncated { "true" } else { "false" },
            );
            for s in &page.items {
                let owner_id = s.owner_id.to_string();
                w.create_element("Upload").write_inner_content(|w| {
                    leaf(w, "Key", s.key.as_str());
                    leaf(w, "UploadId", s.upload_id.as_str());
                    owner(w, &owner_id, &owner_id);
                    // S3 also nests an Initiator block mirroring the owner.
                    w.create_element("Initiator").write_inner_content(|w| {
                        leaf(w, "ID", &owner_id);
                        leaf(w, "DisplayName", &owner_id);
                        Ok(())
                    })?;
                    leaf(w, "StorageClass", "STANDARD");
                    leaf(w, "Initiated", &format_iso8601(s.created_at));
                    Ok(())
                })?;
            }
            write_common_prefixes(w, &page.common_prefixes);
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

// ===========================================================================================
// CopyObject / Delete / Tagging / Versioning
// ===========================================================================================

/// `CopyObjectResult` (the ETag is rendered quoted).
#[must_use]
pub fn copy_object_result(etag: &ETag, last_modified: Timestamp) -> String {
    let mut w = new_doc();
    w.create_element("CopyObjectResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            leaf(w, "LastModified", &format_iso8601(last_modified));
            etag_leaf(w, "ETag", etag);
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

/// `DeleteResult` for the multi-object delete operation, carrying the deleted entries and
/// any per-key errors.
#[must_use]
pub fn delete_result(
    deleted: &[(String, Option<String>)],
    errors: &[(String, String, String)],
) -> String {
    let mut w = new_doc();
    w.create_element("DeleteResult")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            for (key, version_id) in deleted {
                w.create_element("Deleted").write_inner_content(|w| {
                    leaf(w, "Key", key);
                    if let Some(v) = version_id {
                        leaf(w, "VersionId", v);
                    }
                    Ok(())
                })?;
            }
            for (key, code, message) in errors {
                w.create_element("Error").write_inner_content(|w| {
                    leaf(w, "Key", key);
                    leaf(w, "Code", code);
                    leaf(w, "Message", message);
                    Ok(())
                })?;
            }
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

/// `Tagging` document with the supplied tag set.
#[must_use]
pub fn tagging(tags: &[(String, String)]) -> String {
    let mut w = new_doc();
    w.create_element("Tagging")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            w.create_element("TagSet").write_inner_content(|w| {
                for (k, v) in tags {
                    w.create_element("Tag").write_inner_content(|w| {
                        leaf(w, "Key", k);
                        leaf(w, "Value", v);
                        Ok(())
                    })?;
                }
                Ok(())
            })?;
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

/// `VersioningConfiguration` reflecting a bucket's [`VersioningState`].
///
/// `Unversioned` buckets have never been configured, so S3 returns an empty document with
/// no `<Status>` element.
#[must_use]
pub fn versioning_configuration(state: VersioningState) -> String {
    let mut w = new_doc();
    w.create_element("VersioningConfiguration")
        .with_attribute(("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/"))
        .write_inner_content(|w| {
            match state {
                VersioningState::Unversioned => {}
                VersioningState::Enabled => leaf(w, "Status", "Enabled"),
                VersioningState::Suspended => leaf(w, "Status", "Suspended"),
            }
            Ok(())
        })
        .expect("infallible");
    finish(w)
}

#[cfg(test)]
mod tests;
