//! Unit and round-trip tests for the XML codec. Generators are checked for the load-bearing
//! wire shapes; every parser has a round-trip test and a malformed-input test asserting it
//! returns `Err` rather than panicking.

use super::*;
use crate::parse::{
    CorsRule, parse_access_control_policy, parse_complete_multipart, parse_cors_configuration,
    parse_delete, parse_tagging, parse_versioning_configuration,
};
use cairn_types::{
    Bucket, BucketName, ChecksumAlgorithm, ChecksumValue, ETag, Grantee, ListPage,
    MultipartSession, MultipartStatus, ObjectKey, ObjectSummary, OwnershipMode, PartRecord,
    Permission, StorageClass, StoragePath, Timestamp, UploadId, UserId, VersionId, VersioningState,
};

// -------------------------------------------------------------------------------------------
// Builders
// -------------------------------------------------------------------------------------------

fn summary(key: &str, etag: &str, size: u64) -> ObjectSummary {
    ObjectSummary {
        key: ObjectKey::parse(key).unwrap(),
        version_id: VersionId::null(),
        is_latest: true,
        is_delete_marker: false,
        etag: ETag::from_md5_hex(etag.to_owned()),
        size,
        last_modified: Timestamp(1_750_000_000_000),
        storage_class: StorageClass::Standard,
        owner_id: UserId("owner-1".to_owned()),
    }
}

// -------------------------------------------------------------------------------------------
// Generators
// -------------------------------------------------------------------------------------------

#[test]
fn declaration_and_no_bom() {
    let doc = error_document("NoSuchKey", "The key does not exist.", "/b/k", "req-1");
    assert!(doc.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
    assert!(!doc.as_bytes().starts_with(&[0xEF, 0xBB, 0xBF]), "no BOM");
}

#[test]
fn error_document_carries_all_fields() {
    let doc = error_document("AccessDenied", "Denied <&>", "/bucket/key", "req-42");
    assert!(doc.contains("<Code>AccessDenied</Code>"));
    // The message angle brackets/amp must be escaped.
    assert!(doc.contains("<Message>Denied &lt;&amp;&gt;</Message>"));
    assert!(doc.contains("<Resource>/bucket/key</Resource>"));
    assert!(doc.contains("<RequestId>req-42</RequestId>"));
}

#[test]
fn list_objects_v2_shape() {
    let page = ListPage {
        items: vec![summary("a/b.txt", "deadbeef", 17)],
        common_prefixes: vec!["a/".to_owned(), "c/".to_owned()],
        next_cursor: Some("a/b.txt".to_owned()),
        next_version_id_marker: None,
        truncated: true,
    };
    let xml = list_objects_v2(
        "mybucket",
        Some("a/"),
        Some("/"),
        1000,
        &page,
        None,
        None,
        None,
    );

    assert!(xml.contains("<Name>mybucket</Name>"));
    assert!(xml.contains("<Key>a/b.txt</Key>"));
    // ETag must be quoted.
    assert!(xml.contains("<ETag>&quot;deadbeef&quot;</ETag>"));
    assert!(xml.contains("<Size>17</Size>"));
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
    assert!(xml.contains("<NextContinuationToken>a/b.txt</NextContinuationToken>"));
    // CommonPrefixes are grouped, one element each.
    assert!(xml.contains("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>"));
    assert!(xml.contains("<CommonPrefixes><Prefix>c/</Prefix></CommonPrefixes>"));
    // KeyCount = items + common prefixes.
    assert!(xml.contains("<KeyCount>3</KeyCount>"));
    // LastModified is ISO-8601 millis-Z.
    assert!(xml.contains("<LastModified>2025-"));
    assert!(xml.contains("Z</LastModified>"));
}

#[test]
fn list_objects_v1_marker_form() {
    let page = ListPage {
        items: vec![summary("k1", "aa", 1)],
        common_prefixes: vec![],
        next_cursor: Some("k1".to_owned()),
        next_version_id_marker: None,
        truncated: true,
    };
    let xml = list_objects_v1("b", None, None, 100, &page, Some("k0"), None);
    assert!(xml.contains("<Marker>k0</Marker>"));
    assert!(xml.contains("<NextMarker>k1</NextMarker>"));
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
    // V1 has no ContinuationToken.
    assert!(!xml.contains("ContinuationToken"));
}

#[test]
fn list_versions_distinguishes_markers() {
    let mut version = summary("doc", "ff", 9);
    version.version_id = VersionId::from_string("v-100".to_owned());
    version.is_latest = true;
    let mut marker = summary("doc", "ignored", 0);
    marker.is_delete_marker = true;
    marker.is_latest = false;
    marker.version_id = VersionId::from_string("v-099".to_owned());

    let page = ListPage {
        items: vec![version, marker],
        common_prefixes: vec![],
        next_cursor: None,
        next_version_id_marker: None,
        truncated: false,
    };
    let xml = list_object_versions("b", None, None, 1000, &page, None, None, None);
    assert!(xml.contains("<Version>"));
    assert!(xml.contains("<DeleteMarker>"));
    assert!(xml.contains("<VersionId>v-100</VersionId>"));
    assert!(xml.contains("<VersionId>v-099</VersionId>"));
    assert!(xml.contains("<IsLatest>true</IsLatest>"));
    // A delete marker carries no Size element.
    let dm_start = xml.find("<DeleteMarker>").unwrap();
    let dm_end = xml[dm_start..].find("</DeleteMarker>").unwrap() + dm_start;
    assert!(!xml[dm_start..dm_end].contains("<Size>"));
    // A version entry does carry Size + quoted ETag.
    let v_start = xml.find("<Version>").unwrap();
    let v_end = xml[v_start..].find("</Version>").unwrap() + v_start;
    assert!(xml[v_start..v_end].contains("<Size>9</Size>"));
    assert!(xml[v_start..v_end].contains("&quot;ff&quot;"));
}

/// S3 echoes the request's `start-after` back in the V2 listing; a client uses it to confirm which
/// resume point the page answered. Cairn's writer never received the value (2026-07 conformance,
/// finding E).
#[test]
fn list_objects_v2_echoes_start_after() {
    let page = ListPage {
        items: vec![summary("k05", "aa", 1)],
        common_prefixes: vec![],
        next_cursor: None,
        next_version_id_marker: None,
        truncated: false,
    };
    let xml = list_objects_v2("b", None, None, 1000, &page, None, Some("k04"), None);
    assert!(xml.contains("<StartAfter>k04</StartAfter>"), "{xml}");

    // Absent means absent: S3 emits no empty <StartAfter/> when the request omitted it.
    let xml = list_objects_v2("b", None, None, 1000, &page, None, None, None);
    assert!(!xml.contains("StartAfter"), "{xml}");
}

/// The `encoding-type=url` echo and the encoding itself are ONE decision: botocore only URL-decodes
/// a response that echoes `<EncodingType>`, so a listing must emit both halves or neither.
#[test]
fn encoding_type_is_absent_and_fields_raw_without_the_parameter() {
    let page = ListPage {
        items: vec![summary("enc/sp ace.txt", "aa", 1)],
        common_prefixes: vec!["enc/x y/".to_owned()],
        next_cursor: None,
        next_version_id_marker: None,
        truncated: false,
    };
    let xml = list_objects_v2("b", Some("enc/"), None, 1000, &page, None, None, None);
    assert!(!xml.contains("EncodingType"), "{xml}");
    assert!(xml.contains("<Key>enc/sp ace.txt</Key>"), "{xml}");
    assert!(xml.contains("<Prefix>enc/x y/</Prefix>"), "{xml}");
}

#[test]
fn list_objects_v2_url_encoding_encodes_every_key_field() {
    let page = ListPage {
        items: vec![summary("enc/été/sp ace+&.txt", "aa", 1)],
        common_prefixes: vec!["enc/été/x y/".to_owned()],
        next_cursor: Some("dG9rZW4=".to_owned()),
        next_version_id_marker: None,
        truncated: true,
    };
    let xml = list_objects_v2(
        "b",
        Some("enc/été/"),
        Some("|"),
        1000,
        &page,
        Some("cHJldg=="),
        Some("enc/a b"),
        Some(EncodingType::Url),
    );
    assert!(xml.contains("<EncodingType>url</EncodingType>"), "{xml}");
    assert!(
        xml.contains("<Key>enc/%C3%A9t%C3%A9/sp%20ace%2B%26.txt</Key>"),
        "{xml}"
    );
    assert!(xml.contains("<Prefix>enc/%C3%A9t%C3%A9/</Prefix>"), "{xml}");
    assert!(xml.contains("<Delimiter>%7C</Delimiter>"), "{xml}");
    assert!(
        xml.contains("<CommonPrefixes><Prefix>enc/%C3%A9t%C3%A9/x%20y/</Prefix></CommonPrefixes>"),
        "{xml}"
    );
    assert!(xml.contains("<StartAfter>enc/a%20b</StartAfter>"), "{xml}");
    // The continuation tokens are opaque, not key-derived: S3 leaves them alone, and encoding the
    // `=` padding would break a client that echoes the value back verbatim.
    assert!(
        xml.contains("<ContinuationToken>cHJldg==</ContinuationToken>"),
        "{xml}"
    );
    assert!(
        xml.contains("<NextContinuationToken>dG9rZW4=</NextContinuationToken>"),
        "{xml}"
    );
}

#[test]
fn list_objects_v1_url_encoding_encodes_marker_and_next_marker() {
    let page = ListPage {
        items: vec![summary("sp ace.txt", "aa", 1)],
        common_prefixes: vec![],
        next_cursor: Some("sp ace.txt".to_owned()),
        next_version_id_marker: None,
        truncated: true,
    };
    let xml = list_objects_v1(
        "b",
        Some("é/"),
        None,
        100,
        &page,
        Some("k 0"),
        Some(EncodingType::Url),
    );
    assert!(xml.contains("<EncodingType>url</EncodingType>"), "{xml}");
    assert!(xml.contains("<Prefix>%C3%A9/</Prefix>"), "{xml}");
    assert!(xml.contains("<Marker>k%200</Marker>"), "{xml}");
    assert!(
        xml.contains("<NextMarker>sp%20ace.txt</NextMarker>"),
        "{xml}"
    );
    assert!(xml.contains("<Key>sp%20ace.txt</Key>"), "{xml}");
}

#[test]
fn list_object_versions_url_encoding_leaves_version_ids_alone() {
    let mut version = summary("sp ace.txt", "ff", 9);
    version.version_id = VersionId::from_string("v=100".to_owned());
    let page = ListPage {
        items: vec![version],
        common_prefixes: vec![],
        next_cursor: Some("sp ace.txt".to_owned()),
        next_version_id_marker: Some("v=100".to_owned()),
        truncated: true,
    };
    let xml = list_object_versions(
        "b",
        None,
        None,
        1000,
        &page,
        Some("k 0"),
        Some("v=099"),
        Some(EncodingType::Url),
    );
    assert!(xml.contains("<EncodingType>url</EncodingType>"), "{xml}");
    assert!(xml.contains("<KeyMarker>k%200</KeyMarker>"), "{xml}");
    assert!(
        xml.contains("<NextKeyMarker>sp%20ace.txt</NextKeyMarker>"),
        "{xml}"
    );
    assert!(xml.contains("<Key>sp%20ace.txt</Key>"), "{xml}");
    // Version ids are opaque, like the continuation token.
    assert!(
        xml.contains("<VersionIdMarker>v=099</VersionIdMarker>"),
        "{xml}"
    );
    assert!(xml.contains("<VersionId>v=100</VersionId>"), "{xml}");
    assert!(
        xml.contains("<NextVersionIdMarker>v=100</NextVersionIdMarker>"),
        "{xml}"
    );
}

/// The literal set is the RFC 3986 unreserved characters plus `/` — matching what S3 returns
/// (`enc/%C3%A9t%C3%A9/`), so an encoded key still reads as a path.
#[test]
fn percent_encode_matches_the_s3_safe_set() {
    assert_eq!(
        percent_encode("abcXYZ019-_.~/"),
        "abcXYZ019-_.~/",
        "unreserved + the key separator stay literal"
    );
    // A space is %20, never `+`: clients decode with `unquote`, not `unquote_plus`.
    assert_eq!(percent_encode("a b"), "a%20b");
    assert_eq!(percent_encode("a+b%c&d=e"), "a%2Bb%25c%26d%3De");
    // Non-ASCII encodes byte-wise over the UTF-8 form, uppercase hex.
    assert_eq!(percent_encode("日本"), "%E6%97%A5%E6%9C%AC");
    // The characters XML 1.0 cannot carry survive an encoded listing — this is the fallback
    // `ObjectKey::parse`'s comment says does not exist for the unencoded form.
    assert_eq!(percent_encode("a\u{1}b"), "a%01b");
}

#[test]
fn encoding_type_parses_only_url() {
    assert_eq!(EncodingType::parse("url"), Some(EncodingType::Url));
    assert_eq!(EncodingType::parse("URL"), Some(EncodingType::Url));
    assert_eq!(EncodingType::Url.as_str(), "url");
    assert_eq!(EncodingType::parse(""), None);
    assert_eq!(EncodingType::parse("base64"), None);
}

#[test]
fn list_buckets_shape() {
    let buckets = vec![Bucket {
        name: BucketName::parse("my-bucket").unwrap(),
        owner_id: UserId("o".to_owned()),
        created_at: Timestamp(1_700_000_000_000),
        versioning: VersioningState::Enabled,
        ownership_mode: OwnershipMode::BucketOwnerEnforced,
        region: "us-east-1".to_owned(),
        compression: None,
    }];
    let xml = list_buckets("owner-id", "Owner Name", &buckets);
    assert!(xml.contains("<ID>owner-id</ID>"));
    assert!(xml.contains("<DisplayName>Owner Name</DisplayName>"));
    assert!(xml.contains("<Name>my-bucket</Name>"));
    assert!(xml.contains("<CreationDate>2023-"));
}

#[test]
fn multipart_generators() {
    let init = initiate_multipart_result("b", "k", "up-1");
    assert!(init.contains("<Bucket>b</Bucket>"));
    assert!(init.contains("<UploadId>up-1</UploadId>"));

    let etag = ETag::multipart("abc".to_owned(), 3);
    let comp = complete_multipart_result("http://h/b/k", "b", "k", &etag, None, None);
    assert!(comp.contains("<Location>http://h/b/k</Location>"));
    assert!(comp.contains("<ETag>&quot;abc-3&quot;</ETag>"));
    // No supplementary checksum -> no checksum elements.
    assert!(!comp.contains("<ChecksumType>"));
    assert!(!comp.contains("<ChecksumCRC32>"));

    // A completed object with a COMPOSITE object checksum renders the algo element + type.
    let cv = ChecksumValue {
        algorithm: ChecksumAlgorithm::Crc32,
        value: "AAAAAA==-2".to_owned(),
    };
    let comp = complete_multipart_result(
        "http://h/b/k",
        "b",
        "k",
        &etag,
        Some(&cv),
        Some("COMPOSITE"),
    );
    assert!(
        comp.contains("<ChecksumCRC32>AAAAAA==-2</ChecksumCRC32>"),
        "{comp}"
    );
    assert!(
        comp.contains("<ChecksumType>COMPOSITE</ChecksumType>"),
        "{comp}"
    );
}

#[test]
fn list_parts_shape() {
    let page = ListPage {
        items: vec![PartRecord {
            part_number: 1,
            size: 100,
            etag: "p1etag".to_owned(),
            storage_path: StoragePath::from_string("b/uuid".to_owned()),
            checksum: None,
        }],
        common_prefixes: vec![],
        next_cursor: None,
        next_version_id_marker: None,
        truncated: false,
    };
    let xml = list_parts_result("b", "k", "up", &page, "owner", 0, 1000);
    assert!(xml.contains("<PartNumber>1</PartNumber>"));
    assert!(xml.contains("<ETag>&quot;p1etag&quot;</ETag>"));
    assert!(xml.contains("<Size>100</Size>"));
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
}

#[test]
fn list_multipart_uploads_shape() {
    let session = MultipartSession {
        upload_id: UploadId::from_string("up-9".to_owned()),
        bucket: BucketName::parse("my-bucket").unwrap(),
        key: ObjectKey::parse("big.bin").unwrap(),
        content_type: "application/octet-stream".to_owned(),
        status: MultipartStatus::Active,
        owner_id: UserId("o".to_owned()),
        intended_acl: None,
        user_metadata: vec![],
        sse_requested: false,
        created_at: Timestamp(1_750_000_000_000),
        updated_at: Timestamp(1_750_000_000_000),
    };
    let page = ListPage {
        items: vec![session],
        common_prefixes: vec!["pre/".to_owned()],
        next_cursor: None,
        next_version_id_marker: None,
        truncated: false,
    };
    let xml = list_multipart_uploads_result(
        "my-bucket",
        Some("b"),
        Some("/"),
        &page,
        None,
        None,
        1000,
        None,
    );
    assert!(xml.contains("<Key>big.bin</Key>"));
    assert!(xml.contains("<UploadId>up-9</UploadId>"));
    assert!(xml.contains("<CommonPrefixes><Prefix>pre/</Prefix></CommonPrefixes>"));
    assert!(xml.contains("<Initiated>2025-"));
}

/// Audit 2026-07: a truncated multipart listing must emit BOTH halves of the resume pair. With
/// only `NextKeyMarker` a key holding more uploads than `max-uploads` can never be paged past.
#[test]
fn list_multipart_uploads_truncated_emits_both_markers() {
    let session = MultipartSession {
        upload_id: UploadId::from_string("up-9".to_owned()),
        bucket: BucketName::parse("my-bucket").unwrap(),
        key: ObjectKey::parse("big.bin").unwrap(),
        content_type: "application/octet-stream".to_owned(),
        status: MultipartStatus::Active,
        owner_id: UserId("o".to_owned()),
        intended_acl: None,
        user_metadata: vec![],
        sse_requested: false,
        created_at: Timestamp(1_750_000_000_000),
        updated_at: Timestamp(1_750_000_000_000),
    };
    let page = ListPage {
        items: vec![session],
        common_prefixes: vec![],
        next_cursor: Some("big.bin".to_owned()),
        next_version_id_marker: Some("up-9".to_owned()),
        truncated: true,
    };
    let xml = list_multipart_uploads_result(
        "my-bucket",
        None,
        None,
        &page,
        Some("prev.bin"),
        Some("up-1"),
        1,
        None,
    );
    assert!(xml.contains("<KeyMarker>prev.bin</KeyMarker>"), "{xml}");
    assert!(
        xml.contains("<UploadIdMarker>up-1</UploadIdMarker>"),
        "{xml}"
    );
    assert!(
        xml.contains("<NextKeyMarker>big.bin</NextKeyMarker>"),
        "{xml}"
    );
    assert!(
        xml.contains("<NextUploadIdMarker>up-9</NextUploadIdMarker>"),
        "{xml}"
    );
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"), "{xml}");
}

/// ListMultipartUploads carries `encoding-type` like the object listings do; the upload-id markers
/// are opaque and stay raw.
#[test]
fn list_multipart_uploads_url_encoding_encodes_key_fields() {
    let session = MultipartSession {
        upload_id: UploadId::from_string("up=9".to_owned()),
        bucket: BucketName::parse("my-bucket").unwrap(),
        key: ObjectKey::parse("big file.bin").unwrap(),
        content_type: "application/octet-stream".to_owned(),
        status: MultipartStatus::Active,
        owner_id: UserId("o".to_owned()),
        intended_acl: None,
        user_metadata: vec![],
        sse_requested: false,
        created_at: Timestamp(1_750_000_000_000),
        updated_at: Timestamp(1_750_000_000_000),
    };
    let page = ListPage {
        items: vec![session],
        common_prefixes: vec!["pre fix/".to_owned()],
        next_cursor: None,
        next_version_id_marker: None,
        truncated: false,
    };
    let xml = list_multipart_uploads_result(
        "my-bucket",
        Some("big "),
        Some("/"),
        &page,
        Some("k 0"),
        Some("up=1"),
        1000,
        Some(EncodingType::Url),
    );
    assert!(xml.contains("<EncodingType>url</EncodingType>"), "{xml}");
    assert!(xml.contains("<Key>big%20file.bin</Key>"), "{xml}");
    assert!(xml.contains("<Prefix>big%20</Prefix>"), "{xml}");
    assert!(xml.contains("<KeyMarker>k%200</KeyMarker>"), "{xml}");
    assert!(
        xml.contains("<CommonPrefixes><Prefix>pre%20fix/</Prefix></CommonPrefixes>"),
        "{xml}"
    );
    // Opaque: not key-derived, so not encoded.
    assert!(
        xml.contains("<UploadIdMarker>up=1</UploadIdMarker>"),
        "{xml}"
    );
    assert!(xml.contains("<UploadId>up=9</UploadId>"), "{xml}");
}

#[test]
fn copy_object_result_shape() {
    let etag = ETag::from_md5_hex("cafe".to_owned());
    let xml = copy_object_result(&etag, Timestamp(1_750_000_000_000));
    assert!(xml.contains("<ETag>&quot;cafe&quot;</ETag>"));
    assert!(xml.contains("<LastModified>2025-"));
}

#[test]
fn copy_part_result_shape() {
    let etag = ETag::from_md5_hex("beef".to_owned());
    let xml = copy_part_result(&etag, Timestamp(1_750_000_000_000));
    assert!(xml.contains("<CopyPartResult"));
    assert!(xml.contains("<ETag>&quot;beef&quot;</ETag>"));
    assert!(xml.contains("<LastModified>2025-"));
}

#[test]
fn get_object_attributes_single_part() {
    let etag = ETag::from_md5_hex("d41d8cd9".to_owned());
    let xml = get_object_attributes(&etag, 1234, StorageClass::Standard, &[], None, None);
    // The ETag is rendered UNQUOTED for GetObjectAttributes.
    assert!(xml.contains("<ETag>d41d8cd9</ETag>"), "{xml}");
    assert!(xml.contains("<ObjectSize>1234</ObjectSize>"));
    assert!(xml.contains("<StorageClass>STANDARD</StorageClass>"));
    // No checksum block and no ObjectParts for a single-part object with no checksums.
    assert!(!xml.contains("<Checksum>"));
    assert!(!xml.contains("<ObjectParts>"));
}

#[test]
fn get_object_attributes_with_checksum_and_parts() {
    let etag = ETag::from_md5_hex("abc-2".to_owned());
    let checksums = vec![ChecksumValue {
        algorithm: ChecksumAlgorithm::Crc32c,
        value: "AAAAAA==".to_owned(),
    }];
    let parts: Vec<(u16, u64)> = vec![(1, 5_242_880), (2, 8)];
    let xml = get_object_attributes(
        &etag,
        5_242_888,
        StorageClass::Standard,
        &checksums,
        Some("COMPOSITE"),
        Some(&parts),
    );
    assert!(
        xml.contains(
            "<Checksum><ChecksumCRC32C>AAAAAA==</ChecksumCRC32C><ChecksumType>COMPOSITE</ChecksumType></Checksum>"
        ),
        "{xml}"
    );
    assert!(xml.contains("<ObjectParts><TotalPartsCount>2</TotalPartsCount>"));
    assert!(xml.contains("<Part><PartNumber>1</PartNumber><Size>5242880</Size></Part>"));
    assert!(xml.contains("<Part><PartNumber>2</PartNumber><Size>8</Size></Part>"));
    assert!(xml.contains("<ObjectSize>5242888</ObjectSize>"));
}

#[test]
fn delete_result_shape() {
    let deleted = vec![
        ("a".to_owned(), None, false, None),
        ("b".to_owned(), Some("v1".to_owned()), false, None),
        // A versioned plain delete that inserted a marker: surfaces DeleteMarker + its id.
        ("d".to_owned(), None, true, Some("dmv9".to_owned())),
    ];
    let errors = vec![("c".to_owned(), "AccessDenied".to_owned(), "no".to_owned())];
    let xml = delete_result(&deleted, &errors);
    assert!(xml.contains("<Deleted><Key>a</Key></Deleted>"));
    assert!(xml.contains("<Deleted><Key>b</Key><VersionId>v1</VersionId></Deleted>"));
    assert!(xml.contains(
        "<Deleted><Key>d</Key><DeleteMarker>true</DeleteMarker><DeleteMarkerVersionId>dmv9</DeleteMarkerVersionId></Deleted>"
    ));
    assert!(
        xml.contains("<Error><Key>c</Key><Code>AccessDenied</Code><Message>no</Message></Error>")
    );
}

#[test]
fn versioning_configuration_states() {
    assert!(
        versioning_configuration(VersioningState::Enabled).contains("<Status>Enabled</Status>")
    );
    assert!(
        versioning_configuration(VersioningState::Suspended).contains("<Status>Suspended</Status>")
    );
    // Unversioned emits no Status element.
    assert!(!versioning_configuration(VersioningState::Unversioned).contains("<Status>"));
}

#[test]
fn tagging_escapes_special_chars() {
    let tags = vec![("env".to_owned(), "a&b<c>".to_owned())];
    let xml = tagging(&tags);
    assert!(xml.contains("<Key>env</Key>"));
    assert!(xml.contains("<Value>a&amp;b&lt;c&gt;</Value>"));
}

// -------------------------------------------------------------------------------------------
// Round-trip parsers
// -------------------------------------------------------------------------------------------

#[test]
fn round_trip_complete_multipart() {
    let body = "<CompleteMultipartUpload>\
        <Part><PartNumber>1</PartNumber><ETag>\"etag-one\"</ETag></Part>\
        <Part><PartNumber>2</PartNumber><ETag>\"etag-two\"</ETag></Part>\
        </CompleteMultipartUpload>";
    let parts = parse_complete_multipart(body.as_bytes()).unwrap();
    assert_eq!(
        parts,
        vec![(1, "etag-one".to_owned()), (2, "etag-two".to_owned())]
    );
}

#[test]
fn round_trip_tagging() {
    let original = vec![
        ("env".to_owned(), "prod".to_owned()),
        ("team".to_owned(), "a&b".to_owned()),
    ];
    let xml = tagging(&original);
    let parsed = parse_tagging(xml.as_bytes()).unwrap();
    assert_eq!(parsed, original);
}

#[test]
fn text_split_across_cdata_is_coalesced() {
    // Audit #24: character data split across text/CDATA chunks must coalesce into the full value,
    // not collapse to only the last chunk.
    let body = "<Tagging><TagSet><Tag>\
        <Key>k</Key><Value>foo<![CDATA[bar]]>baz</Value>\
        </Tag></TagSet></Tagging>";
    let tags = parse_tagging(body.as_bytes()).unwrap();
    assert_eq!(tags, vec![("k".to_owned(), "foobarbaz".to_owned())]);

    // Same for a CompleteMultipartUpload ETag split by a CDATA section.
    let mp = "<CompleteMultipartUpload><Part>\
        <PartNumber>1</PartNumber><ETag>\"ab<![CDATA[cd]]>ef\"</ETag>\
        </Part></CompleteMultipartUpload>";
    let parts = parse_complete_multipart(mp.as_bytes()).unwrap();
    assert_eq!(parts, vec![(1, "abcdef".to_owned())]);
}

#[test]
fn validate_tags_enforces_s3_limits() {
    use crate::parse::{MAX_TAGS_BUCKET, MAX_TAGS_OBJECT, validate_tags};
    fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
        p.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
    fn assert_invalid<T: std::fmt::Debug>(r: Result<T, cairn_types::Error>) {
        assert!(
            matches!(r, Err(cairn_types::Error::InvalidTag(_))),
            "expected InvalidTag, got {r:?}"
        );
    }

    // Count limits: 10 ok / 11 reject for objects; 50 ok / 51 reject for buckets.
    let n = |c: usize| -> Vec<(String, String)> {
        (0..c).map(|i| (format!("k{i}"), "v".to_owned())).collect()
    };
    assert!(validate_tags(&n(10), MAX_TAGS_OBJECT).is_ok());
    assert_invalid(validate_tags(&n(11), MAX_TAGS_OBJECT));
    assert!(validate_tags(&n(50), MAX_TAGS_BUCKET).is_ok());
    assert_invalid(validate_tags(&n(51), MAX_TAGS_BUCKET));

    // Length boundaries counted in Unicode scalars (multibyte proves it is not byte length).
    let k128 = "é".repeat(128);
    assert!(validate_tags(&pairs(&[(&k128, "v")]), MAX_TAGS_OBJECT).is_ok());
    let k129 = "é".repeat(129);
    assert_invalid(validate_tags(&pairs(&[(&k129, "v")]), MAX_TAGS_OBJECT));
    let v256 = "x".repeat(256);
    assert!(validate_tags(&pairs(&[("k", &v256)]), MAX_TAGS_OBJECT).is_ok());
    let v257 = "x".repeat(257);
    assert_invalid(validate_tags(&pairs(&[("k", &v257)]), MAX_TAGS_OBJECT));

    // Charset, empty key, duplicate key, reserved aws: prefix.
    assert_invalid(validate_tags(&pairs(&[("bad*key", "v")]), MAX_TAGS_OBJECT));
    assert_invalid(validate_tags(&pairs(&[("", "v")]), MAX_TAGS_OBJECT));
    assert_invalid(validate_tags(
        &pairs(&[("dup", "1"), ("dup", "2")]),
        MAX_TAGS_OBJECT,
    ));
    assert_invalid(validate_tags(&pairs(&[("aws:foo", "v")]), MAX_TAGS_OBJECT));
    assert_invalid(validate_tags(&pairs(&[("AWS:foo", "v")]), MAX_TAGS_OBJECT));

    // All permitted punctuation + spaces + unicode letters pass.
    assert!(validate_tags(&pairs(&[("a+b-c=d.e_f:g/h@i", "v 1 é")]), MAX_TAGS_OBJECT).is_ok());
}

#[test]
fn round_trip_versioning_configuration() {
    for state in [
        VersioningState::Enabled,
        VersioningState::Suspended,
        VersioningState::Unversioned,
    ] {
        let xml = versioning_configuration(state);
        assert_eq!(
            parse_versioning_configuration(xml.as_bytes()).unwrap(),
            state
        );
    }
}

#[test]
fn round_trip_delete() {
    let body = "<Delete><Quiet>true</Quiet>\
        <Object><Key>a</Key></Object>\
        <Object><Key>b</Key><VersionId>v1</VersionId></Object>\
        </Delete>";
    let (quiet, objects) = parse_delete(body.as_bytes()).unwrap();
    assert!(quiet);
    assert_eq!(
        objects,
        vec![
            ("a".to_owned(), None),
            ("b".to_owned(), Some("v1".to_owned()))
        ]
    );
}

#[test]
fn delete_default_not_quiet() {
    let body = "<Delete><Object><Key>x</Key></Object></Delete>";
    let (quiet, objects) = parse_delete(body.as_bytes()).unwrap();
    assert!(!quiet);
    assert_eq!(objects, vec![("x".to_owned(), None)]);
}

#[test]
fn round_trip_cors() {
    let body = "<CORSConfiguration>\
        <CORSRule>\
            <AllowedOrigin>https://example.com</AllowedOrigin>\
            <AllowedOrigin>*</AllowedOrigin>\
            <AllowedMethod>GET</AllowedMethod>\
            <AllowedMethod>PUT</AllowedMethod>\
            <AllowedHeader>*</AllowedHeader>\
            <ExposeHeader>ETag</ExposeHeader>\
            <MaxAgeSeconds>3600</MaxAgeSeconds>\
        </CORSRule>\
        <CORSRule><AllowedOrigin>https://x.io</AllowedOrigin><AllowedMethod>DELETE</AllowedMethod></CORSRule>\
        </CORSConfiguration>";
    let rules = parse_cors_configuration(body.as_bytes()).unwrap();
    assert_eq!(rules.len(), 2);
    assert_eq!(
        rules[0],
        CorsRule {
            allowed_origins: vec!["https://example.com".to_owned(), "*".to_owned()],
            allowed_methods: vec!["GET".to_owned(), "PUT".to_owned()],
            allowed_headers: vec!["*".to_owned()],
            expose_headers: vec!["ETag".to_owned()],
            max_age_seconds: Some(3600),
        }
    );
    assert_eq!(rules[1].allowed_origins, vec!["https://x.io".to_owned()]);
    assert_eq!(rules[1].allowed_methods, vec!["DELETE".to_owned()]);
    assert_eq!(rules[1].max_age_seconds, None);
}

#[test]
fn cors_unescapes_entities() {
    let body = "<CORSConfiguration><CORSRule>\
        <AllowedOrigin>https://a.example?x=1&amp;y=2</AllowedOrigin>\
        <AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    let rules = parse_cors_configuration(body.as_bytes()).unwrap();
    assert_eq!(rules[0].allowed_origins[0], "https://a.example?x=1&y=2");
}

#[test]
fn round_trip_access_control_policy() {
    let body = "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
        <Owner><ID>owner-1</ID><DisplayName>owner-1</DisplayName></Owner>\
        <AccessControlList>\
            <Grant>\
                <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
                    <ID>owner-1</ID><DisplayName>owner-1</DisplayName>\
                </Grantee>\
                <Permission>FULL_CONTROL</Permission>\
            </Grant>\
            <Grant>\
                <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">\
                    <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>\
                </Grantee>\
                <Permission>READ</Permission>\
            </Grant>\
            <Grant>\
                <Grantee xsi:type=\"Group\">\
                    <URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI>\
                </Grantee>\
                <Permission>WRITE</Permission>\
            </Grant>\
            <Grant>\
                <Grantee xsi:type=\"CanonicalUser\"><ID>reader-9</ID></Grantee>\
                <Permission>READ_ACP</Permission>\
            </Grant>\
        </AccessControlList>\
        </AccessControlPolicy>";
    let acl = parse_access_control_policy(body.as_bytes()).unwrap();
    assert_eq!(acl.owner, UserId("owner-1".to_owned()));
    assert_eq!(acl.grants.len(), 4);
    assert_eq!(
        acl.grants[0].grantee,
        Grantee::User(UserId("owner-1".to_owned()))
    );
    assert_eq!(acl.grants[0].permission, Permission::FullControl);
    assert_eq!(acl.grants[1].grantee, Grantee::AllUsers);
    assert_eq!(acl.grants[1].permission, Permission::Read);
    assert_eq!(acl.grants[2].grantee, Grantee::AuthenticatedUsers);
    assert_eq!(acl.grants[2].permission, Permission::Write);
    assert_eq!(
        acl.grants[3].grantee,
        Grantee::User(UserId("reader-9".to_owned()))
    );
    assert_eq!(acl.grants[3].permission, Permission::ReadAcp);
}

#[test]
fn access_control_policy_log_delivery_and_write_acp() {
    let body = "<AccessControlPolicy>\
        <Owner><ID>o</ID></Owner>\
        <AccessControlList>\
            <Grant><Grantee xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/s3/LogDelivery</URI></Grantee>\
                <Permission>WRITE_ACP</Permission></Grant>\
        </AccessControlList>\
        </AccessControlPolicy>";
    let acl = parse_access_control_policy(body.as_bytes()).unwrap();
    assert_eq!(acl.grants[0].grantee, Grantee::LogDelivery);
    assert_eq!(acl.grants[0].permission, Permission::WriteAcp);
}

#[test]
fn access_control_policy_empty_grant_list() {
    let body = "<AccessControlPolicy><Owner><ID>o</ID></Owner>\
        <AccessControlList></AccessControlList></AccessControlPolicy>";
    let acl = parse_access_control_policy(body.as_bytes()).unwrap();
    assert_eq!(acl.owner, UserId("o".to_owned()));
    assert!(acl.grants.is_empty());
}

// -------------------------------------------------------------------------------------------
// Malformed-input: every parser returns Err, never panics
// -------------------------------------------------------------------------------------------

fn assert_malformed<T: std::fmt::Debug>(r: Result<T, cairn_types::Error>) {
    match r {
        Err(cairn_types::Error::MalformedXml) => {}
        other => panic!("expected MalformedXml, got {other:?}"),
    }
}

#[test]
fn malformed_complete_multipart() {
    // Unbalanced tags.
    assert_malformed(parse_complete_multipart(b"<CompleteMultipartUpload><Part>"));
    // Non-numeric part number.
    assert_malformed(parse_complete_multipart(
        b"<CompleteMultipartUpload><Part><PartNumber>x</PartNumber><ETag>\"e\"</ETag></Part></CompleteMultipartUpload>",
    ));
    // Missing ETag inside Part.
    assert_malformed(parse_complete_multipart(
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>",
    ));
    // Invalid UTF-8.
    assert_malformed(parse_complete_multipart(&[0xff, 0xfe, 0x00]));
    // Part number out of u16 range.
    assert_malformed(parse_complete_multipart(
        b"<CompleteMultipartUpload><Part><PartNumber>99999</PartNumber><ETag>\"e\"</ETag></Part></CompleteMultipartUpload>",
    ));
}

#[test]
fn malformed_delete() {
    assert_malformed(parse_delete(b"<Delete><Object>"));
    // Object without a Key.
    assert_malformed(parse_delete(
        b"<Delete><Object><VersionId>v</VersionId></Object></Delete>",
    ));
    assert_malformed(parse_delete(&[0xff, 0x00, 0xfe]));
    // Mismatched end tag: quick-xml's check_end_names rejects this.
    assert_malformed(parse_delete(
        b"<Delete><Object><Key>k</Key></Wrong></Delete>",
    ));
}

#[test]
fn malformed_tagging() {
    assert_malformed(parse_tagging(b"<Tagging><TagSet><Tag>"));
    // Tag missing Value.
    assert_malformed(parse_tagging(
        b"<Tagging><TagSet><Tag><Key>k</Key></Tag></TagSet></Tagging>",
    ));
    assert_malformed(parse_tagging(&[0xc3, 0x28]));
}

#[test]
fn malformed_versioning() {
    // Unknown status value.
    assert_malformed(parse_versioning_configuration(
        b"<VersioningConfiguration><Status>Bogus</Status></VersioningConfiguration>",
    ));
    // Unbalanced.
    assert_malformed(parse_versioning_configuration(
        b"<VersioningConfiguration><Status>",
    ));
    assert_malformed(parse_versioning_configuration(&[0xff]));
}

#[test]
fn malformed_cors() {
    assert_malformed(parse_cors_configuration(b"<CORSConfiguration><CORSRule>"));
    // Non-numeric MaxAgeSeconds.
    assert_malformed(parse_cors_configuration(
        b"<CORSConfiguration><CORSRule><MaxAgeSeconds>soon</MaxAgeSeconds></CORSRule></CORSConfiguration>",
    ));
    assert_malformed(parse_cors_configuration(&[0xff, 0xff]));
}

#[test]
fn malformed_access_control_policy() {
    // Owner present but no ID.
    assert_malformed(parse_access_control_policy(
        b"<AccessControlPolicy><Owner></Owner><AccessControlList></AccessControlList></AccessControlPolicy>",
    ));
    // Grant with an unknown permission token.
    assert_malformed(parse_access_control_policy(
        b"<AccessControlPolicy><Owner><ID>o</ID></Owner><AccessControlList>\
          <Grant><Grantee xsi:type=\"CanonicalUser\"><ID>u</ID></Grantee><Permission>BOGUS</Permission></Grant>\
          </AccessControlList></AccessControlPolicy>",
    ));
    // Grant with a grantee but no permission.
    assert_malformed(parse_access_control_policy(
        b"<AccessControlPolicy><Owner><ID>o</ID></Owner><AccessControlList>\
          <Grant><Grantee xsi:type=\"CanonicalUser\"><ID>u</ID></Grantee></Grant>\
          </AccessControlList></AccessControlPolicy>",
    ));
    // Grant with a permission but no grantee identity.
    assert_malformed(parse_access_control_policy(
        b"<AccessControlPolicy><Owner><ID>o</ID></Owner><AccessControlList>\
          <Grant><Grantee xsi:type=\"CanonicalUser\"></Grantee><Permission>READ</Permission></Grant>\
          </AccessControlList></AccessControlPolicy>",
    ));
    // Unknown group URI.
    assert_malformed(parse_access_control_policy(
        b"<AccessControlPolicy><Owner><ID>o</ID></Owner><AccessControlList>\
          <Grant><Grantee xsi:type=\"Group\"><URI>http://example.com/bogus</URI></Grantee><Permission>READ</Permission></Grant>\
          </AccessControlList></AccessControlPolicy>",
    ));
    // Unbalanced.
    assert_malformed(parse_access_control_policy(
        b"<AccessControlPolicy><Owner><ID>o</ID>",
    ));
    // Invalid UTF-8.
    assert_malformed(parse_access_control_policy(&[0xff, 0xfe]));
}

// -------------------------------------------------------------------------------------------
// ETag quoting helper
// -------------------------------------------------------------------------------------------

#[test]
fn etag_quoting_is_a_single_pair() {
    let etag = ETag::from_md5_hex("abc123".to_owned());
    let xml = copy_object_result(&etag, Timestamp(0));
    // Exactly the quoted form, escaped.
    assert!(xml.contains("<ETag>&quot;abc123&quot;</ETag>"));
    // Round-trip the multipart-quote stripping path.
    let parts = parse_complete_multipart(
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"abc123\"</ETag></Part></CompleteMultipartUpload>",
    )
    .unwrap();
    assert_eq!(parts[0].1, "abc123");
}

// -------------------------------------------------------------------------------------------
// Object Lock codecs (retention / legal hold / bucket config) + ISO8601 parse
// -------------------------------------------------------------------------------------------

#[test]
fn iso8601_parse_round_trips_format() {
    use cairn_types::Timestamp;
    // format_iso8601 -> parse_iso8601 is the identity at second granularity.
    let ts = Timestamp(1_700_000_000_000);
    let s = format_iso8601(ts);
    assert_eq!(parse_iso8601(&s), Some(ts));
    // A fractional-seconds suffix is accepted and truncated to the second.
    assert_eq!(
        parse_iso8601("2023-11-14T22:13:20.500Z"),
        Some(Timestamp(1_700_000_000_000)),
    );
    // Garbage is rejected (None, never a panic).
    assert_eq!(parse_iso8601("not-a-date"), None);
    assert_eq!(parse_iso8601(""), None);
}

#[test]
fn retention_codec_round_trips() {
    use cairn_types::{ObjectLockMode, Timestamp};
    let body = b"<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>";
    let r = parse_retention(body).unwrap();
    assert!(matches!(r.mode, ObjectLockMode::Compliance));
    assert!(r.retain_until.0 > Timestamp(1_700_000_000_000).0);
    // Serialize and re-parse: stable.
    let xml = retention_to_xml(Some(&r));
    let r2 = parse_retention(xml.as_bytes()).unwrap();
    assert_eq!(r.mode, r2.mode);
    assert_eq!(r.retain_until, r2.retain_until);
    // Governance variant.
    let g = parse_retention(
        b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>",
    )
    .unwrap();
    assert!(matches!(g.mode, ObjectLockMode::Governance));
    // None renders an empty <Retention/>.
    assert!(retention_to_xml(None).contains("Retention"));
}

#[test]
fn retention_malformed_is_error_not_panic() {
    // Bad mode token.
    assert_malformed(parse_retention(
        b"<Retention><Mode>NOPE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>",
    ));
    // Bad date.
    assert_malformed(parse_retention(
        b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>tomorrow</RetainUntilDate></Retention>",
    ));
    // Invalid UTF-8.
    assert_malformed(parse_retention(&[0xff, 0xfe]));
}

#[test]
fn legal_hold_codec_round_trips() {
    assert!(parse_legal_hold(b"<LegalHold><Status>ON</Status></LegalHold>").unwrap());
    assert!(!parse_legal_hold(b"<LegalHold><Status>OFF</Status></LegalHold>").unwrap());
    // Serialize round-trips.
    assert!(parse_legal_hold(legal_hold_to_xml(true).as_bytes()).unwrap());
    assert!(!parse_legal_hold(legal_hold_to_xml(false).as_bytes()).unwrap());
}

#[test]
fn object_lock_configuration_codec_round_trips() {
    use cairn_types::{ObjectLockConfiguration, ObjectLockMode, RetentionPeriod};
    let body = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                 <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>30</Days></DefaultRetention></Rule>\
                 </ObjectLockConfiguration>";
    let cfg = parse_object_lock_configuration(body).unwrap();
    assert!(cfg.enabled);
    let dr = cfg.default_retention.unwrap();
    assert!(matches!(dr.mode, ObjectLockMode::Governance));
    assert!(matches!(dr.period, RetentionPeriod::Days(30)));
    // Serialize and re-parse.
    let xml = object_lock_configuration_to_xml(&cfg);
    let cfg2 = parse_object_lock_configuration(xml.as_bytes()).unwrap();
    assert_eq!(cfg, cfg2);
    // Years variant.
    let y = parse_object_lock_configuration(
        b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
          <Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Years>7</Years></DefaultRetention></Rule>\
          </ObjectLockConfiguration>",
    )
    .unwrap();
    assert!(matches!(
        y.default_retention.unwrap().period,
        RetentionPeriod::Years(7)
    ));
    // Enabled-only (no default retention) round-trips.
    let enabled_only = ObjectLockConfiguration {
        enabled: true,
        default_retention: None,
    };
    let xml = object_lock_configuration_to_xml(&enabled_only);
    assert_eq!(
        parse_object_lock_configuration(xml.as_bytes()).unwrap(),
        enabled_only
    );
}

#[test]
fn xml_safe_neutralizes_illegal_chars() {
    // Audit 2026-07: XML-1.0-illegal characters must be replaced at the codec boundary so a
    // generator can never emit a non-well-formed document (e.g. a rejected key echoed in DeleteResult).
    assert_eq!(xml_safe("hello/world"), "hello/world"); // unchanged (borrowed)
    assert_eq!(xml_safe("a\u{1}b"), "a\u{FFFD}b"); // C0 control -> replacement char
    assert_eq!(xml_safe("a\u{FFFF}b"), "a\u{FFFD}b"); // U+FFFF -> replacement char
    assert_eq!(xml_safe("a\u{FFFE}b"), "a\u{FFFD}b");
    assert_eq!(xml_safe("a\tb\nc\rd"), "a\tb\nc\rd"); // legal whitespace controls pass through
}

// -------------------------------------------------------------------------------------------
// AWS-STS wire surface (ARCH 14)
// -------------------------------------------------------------------------------------------

#[test]
fn get_session_token_response_wire_shape() {
    // The exact element names, `2011-06-15` default namespace, ISO-8601 `Expiration`, and
    // `ResponseMetadata/RequestId` the SDK credential providers parse. A drift here silently breaks
    // every SDK/Terraform client, so this golden pins the whole document.
    let xml = get_session_token_response(
        "CAIRNTMPABC",
        "sekret",
        "tok.en",
        Timestamp(1_700_000_000_000),
        "req-123",
    );
    assert_eq!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <GetSessionTokenResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
         <GetSessionTokenResult>\
         <Credentials>\
         <AccessKeyId>CAIRNTMPABC</AccessKeyId>\
         <SecretAccessKey>sekret</SecretAccessKey>\
         <SessionToken>tok.en</SessionToken>\
         <Expiration>2023-11-14T22:13:20.000Z</Expiration>\
         </Credentials>\
         </GetSessionTokenResult>\
         <ResponseMetadata><RequestId>req-123</RequestId></ResponseMetadata>\
         </GetSessionTokenResponse>"
    );
}

#[test]
fn assume_role_response_wire_shape() {
    // Adds the echoed `AssumedRoleUser` (`AssumedRoleId` + `Arn`) Terraform's assume_role{} expects,
    // alongside the same `Credentials` block.
    let xml = assume_role_response(
        "CAIRNTMPXYZ",
        "sekret",
        "tok.en",
        Timestamp(1_700_000_000_000),
        "CAIRNTMPXYZ:sess",
        "arn:aws:sts::cairn:assumed-role/deployer/sess",
        "req-456",
    );
    assert_eq!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
         <AssumeRoleResult>\
         <Credentials>\
         <AccessKeyId>CAIRNTMPXYZ</AccessKeyId>\
         <SecretAccessKey>sekret</SecretAccessKey>\
         <SessionToken>tok.en</SessionToken>\
         <Expiration>2023-11-14T22:13:20.000Z</Expiration>\
         </Credentials>\
         <AssumedRoleUser>\
         <AssumedRoleId>CAIRNTMPXYZ:sess</AssumedRoleId>\
         <Arn>arn:aws:sts::cairn:assumed-role/deployer/sess</Arn>\
         </AssumedRoleUser>\
         </AssumeRoleResult>\
         <ResponseMetadata><RequestId>req-456</RequestId></ResponseMetadata>\
         </AssumeRoleResponse>"
    );
}

#[test]
fn sts_error_document_wire_shape() {
    // The query-protocol `<ErrorResponse>` shape (Type=Sender), which is distinct from the S3
    // `<Error>` document; botocore keys retry/refresh behaviour off the `Code`.
    let xml = sts_error_document("InvalidParameterValue", "duration out of range", "req-err");
    assert_eq!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
         <Error>\
         <Type>Sender</Type>\
         <Code>InvalidParameterValue</Code>\
         <Message>duration out of range</Message>\
         </Error>\
         <RequestId>req-err</RequestId>\
         </ErrorResponse>"
    );
}

// -------------------------------------------------------------------------------------------
// ServerSideEncryptionConfiguration
// -------------------------------------------------------------------------------------------

#[test]
fn parse_sse_config_aes256() {
    let body = b"<ServerSideEncryptionConfiguration><Rule>\
        <ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
        </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let rule = crate::parse::parse_server_side_encryption_configuration(body).unwrap();
    assert_eq!(rule.sse_algorithm, "AES256");
    assert_eq!(rule.kms_master_key_id, None);
    assert!(!rule.bucket_key_enabled);
}

#[test]
fn parse_sse_config_kms_with_key_id() {
    let body = b"<ServerSideEncryptionConfiguration><Rule>\
        <ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm>\
        <KMSMasterKeyID>alias/my-key</KMSMasterKeyID>\
        </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
    let rule = crate::parse::parse_server_side_encryption_configuration(body).unwrap();
    assert_eq!(rule.sse_algorithm, "aws:kms");
    assert_eq!(rule.kms_master_key_id.as_deref(), Some("alias/my-key"));
    assert!(!rule.bucket_key_enabled);
}

#[test]
fn sse_config_bucket_key_enabled_round_trip() {
    // Generate with BucketKeyEnabled, then parse it back and confirm the flag survives.
    let xml = server_side_encryption_configuration("aws:kms", Some("k-1"), true);
    let rule = crate::parse::parse_server_side_encryption_configuration(xml.as_bytes()).unwrap();
    assert_eq!(rule.sse_algorithm, "aws:kms");
    assert_eq!(rule.kms_master_key_id.as_deref(), Some("k-1"));
    assert!(
        rule.bucket_key_enabled,
        "BucketKeyEnabled must survive a generate -> parse round-trip"
    );
}

#[test]
fn sse_config_generator_wire_shape() {
    let xml = server_side_encryption_configuration("AES256", None, false);
    assert_eq!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ServerSideEncryptionConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule>\
         <ApplyServerSideEncryptionByDefault>\
         <SSEAlgorithm>AES256</SSEAlgorithm>\
         </ApplyServerSideEncryptionByDefault>\
         <BucketKeyEnabled>false</BucketKeyEnabled>\
         </Rule>\
         </ServerSideEncryptionConfiguration>"
    );
}

#[test]
fn parse_sse_config_rejects_malformed() {
    // Unknown algorithm.
    assert_malformed(crate::parse::parse_server_side_encryption_configuration(
        b"<ServerSideEncryptionConfiguration><Rule>\
          <ApplyServerSideEncryptionByDefault><SSEAlgorithm>rot13</SSEAlgorithm>\
          </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>",
    ));
    // Missing SSEAlgorithm entirely.
    assert_malformed(crate::parse::parse_server_side_encryption_configuration(
        b"<ServerSideEncryptionConfiguration><Rule>\
          <ApplyServerSideEncryptionByDefault></ApplyServerSideEncryptionByDefault>\
          </Rule></ServerSideEncryptionConfiguration>",
    ));
    // Unbalanced / truncated body.
    assert_malformed(crate::parse::parse_server_side_encryption_configuration(
        b"<ServerSideEncryptionConfiguration><Rule>",
    ));
    // Invalid UTF-8.
    assert_malformed(crate::parse::parse_server_side_encryption_configuration(&[
        0xff, 0xff,
    ]));
}
