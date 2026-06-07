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
        truncated: true,
    };
    let xml = list_objects_v2("mybucket", Some("a/"), Some("/"), 1000, &page, None);

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
        truncated: true,
    };
    let xml = list_objects_v1("b", None, None, 100, &page, Some("k0"));
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
        truncated: false,
    };
    let xml = list_object_versions("b", None, None, 1000, &page, None, None);
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
    let comp = complete_multipart_result("http://h/b/k", "b", "k", &etag);
    assert!(comp.contains("<Location>http://h/b/k</Location>"));
    assert!(comp.contains("<ETag>&quot;abc-3&quot;</ETag>"));
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
        created_at: Timestamp(1_750_000_000_000),
        updated_at: Timestamp(1_750_000_000_000),
    };
    let page = ListPage {
        items: vec![session],
        common_prefixes: vec!["pre/".to_owned()],
        next_cursor: None,
        truncated: false,
    };
    let xml =
        list_multipart_uploads_result("my-bucket", Some("b"), Some("/"), &page, None, None, 1000);
    assert!(xml.contains("<Key>big.bin</Key>"));
    assert!(xml.contains("<UploadId>up-9</UploadId>"));
    assert!(xml.contains("<CommonPrefixes><Prefix>pre/</Prefix></CommonPrefixes>"));
    assert!(xml.contains("<Initiated>2025-"));
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
    let xml = get_object_attributes(&etag, 1234, StorageClass::Standard, &[], None);
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
        Some(&parts),
    );
    assert!(
        xml.contains("<Checksum><ChecksumCRC32C>AAAAAA==</ChecksumCRC32C></Checksum>"),
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
        ("a".to_owned(), None),
        ("b".to_owned(), Some("v1".to_owned())),
    ];
    let errors = vec![("c".to_owned(), "AccessDenied".to_owned(), "no".to_owned())];
    let xml = delete_result(&deleted, &errors);
    assert!(xml.contains("<Deleted><Key>a</Key></Deleted>"));
    assert!(xml.contains("<Deleted><Key>b</Key><VersionId>v1</VersionId></Deleted>"));
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
