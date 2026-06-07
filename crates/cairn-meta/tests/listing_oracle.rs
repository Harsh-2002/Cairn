//! Property test for `list_current` (ARCH §29.2, GAPS Medium #11).
//!
//! The example-based `listing_prefix_delimiter_and_pagination` test in `tests/store.rs` pins a
//! few hand-picked cases. This file adds the reference-oracle property the audit found missing:
//! for a *random* set of keys and a *random* `(prefix, delimiter, max-keys)` triple, paging the
//! real SQLite listing to exhaustion must yield exactly what a plain in-memory oracle yields —
//! across every page boundary, not just within a single page.
//!
//! The oracle is a deliberately naive sorted filter: no seek arithmetic, no batching, no cursor
//! successor logic. If the seek-and-batch implementation in `store::list_impl` ever disagrees
//! with it (a dropped key at a page edge, a double-counted common prefix, an off-by-one page
//! fill), the property fails with a shrunk counterexample.

use cairn_types::object::{CompressionDescriptor, ETag, ObjectVersionRow, StorageClass};
use cairn_types::traits::MetadataStore;
use cairn_types::*;
use proptest::collection::{btree_set, vec};
use proptest::prelude::*;

/// Build a minimal current-version row for `key`. Only the fields listing reads (key, latest,
/// not-a-delete-marker) matter; the rest are placeholders.
fn row(bucket: &BucketName, key: &str) -> ObjectVersionRow {
    ObjectVersionRow {
        id: uuid::Uuid::new_v4().simple().to_string(),
        bucket: bucket.clone(),
        key: ObjectKey::parse(key).unwrap(),
        version_id: VersionId::null(),
        is_latest: true,
        is_delete_marker: false,
        size_logical: 1,
        size_physical: 1,
        etag: ETag::from_string("e".to_owned()),
        content_type: "application/octet-stream".to_owned(),
        storage_path: Some(StoragePath::generate(bucket)),
        compression: CompressionDescriptor::Uncompressed,
        storage_class: StorageClass::Standard,
        cold_locator: None,
        owner_id: UserId::generate(),
        user_metadata: Vec::new(),
        acl: None,
        checksums: Vec::new(),
        replication_status: None,
        created_at: Timestamp(1),
        updated_at: Timestamp(1),
    }
}

fn put(row: ObjectVersionRow) -> Mutation {
    Mutation::PutObjectVersion {
        row: Box::new(row),
        precondition: Precondition::default(),
        replication: None,
    }
}

/// The reference listing: a naive sorted filter mirroring the S3 semantics that
/// `store::list_impl` accelerates with seeks and batching. Returns the flat sequence of
/// "entries" a *fully drained* listing must produce, where an entry is either a direct object
/// `Item(key)` or a `CommonPrefix(cp)`. Pagination is then just chunking this sequence by
/// `max_keys`, which is exactly what we assert the real store reproduces.
fn oracle_entries(keys: &[String], prefix: &str, delimiter: Option<&str>) -> Vec<Entry> {
    let mut sorted: Vec<&String> = keys.iter().filter(|k| k.starts_with(prefix)).collect();
    sorted.sort();
    sorted.dedup();

    let mut out: Vec<Entry> = Vec::new();
    let mut seen_cp: Vec<String> = Vec::new();
    for key in sorted {
        match delimiter {
            Some(delim) if !delim.is_empty() => {
                let rest = &key[prefix.len()..];
                if let Some(idx) = rest.find(delim) {
                    let cp = format!("{}{}{}", prefix, &rest[..idx], delim);
                    if !seen_cp.contains(&cp) {
                        seen_cp.push(cp.clone());
                        out.push(Entry::CommonPrefix(cp));
                    }
                    // A key rolled up into a common prefix is never a direct item.
                    continue;
                }
                out.push(Entry::Item(key.clone()));
            }
            _ => out.push(Entry::Item(key.clone())),
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    Item(String),
    CommonPrefix(String),
}

/// Drive the real store: page `list_current` to exhaustion with `max_keys` per page, following
/// `next_cursor`, and flatten the pages back into one `Entry` sequence in listing order. Also
/// returns the number of pages so the property can assert pagination actually occurred.
async fn drain_store(
    store: &cairn_meta::SqliteMetadataStore,
    bucket: &BucketName,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: u32,
) -> (Vec<Entry>, usize) {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let query = ListQuery {
            prefix: (!prefix.is_empty()).then(|| prefix.to_owned()),
            delimiter: delimiter.map(str::to_owned),
            cursor: cursor.clone(),
            start_after: None,
            limit: max_keys,
        };
        let page = store.list_current(bucket, &query).await.unwrap();
        pages += 1;

        // Within a page, items and common-prefixes are each individually sorted and do not
        // interleave in the wire response; the oracle order is the merge of the two by key. We
        // reconstruct the merged order by sorting the page's own entries, which is sound because
        // the full drained sequence is what we ultimately compare (page-internal order of the
        // two disjoint groups is not observable across the cursor boundary).
        let mut page_entries: Vec<Entry> = Vec::new();
        for cp in &page.common_prefixes {
            page_entries.push(Entry::CommonPrefix(cp.clone()));
        }
        for item in &page.items {
            page_entries.push(Entry::Item(item.key.as_str().to_owned()));
        }
        page_entries.sort_by(|a, b| sort_key(a).cmp(sort_key(b)));
        out.extend(page_entries);

        // Each page must respect the max-keys ceiling (items + common prefixes).
        assert!(
            page.items.len() + page.common_prefixes.len() <= max_keys.max(1) as usize,
            "page exceeded max-keys ceiling"
        );

        if !page.truncated {
            break;
        }
        cursor = page.next_cursor.clone();
        assert!(cursor.is_some(), "a truncated page must carry a cursor");
    }
    (out, pages)
}

/// The lexicographic sort key of an entry (the key for an item, the common-prefix string for a
/// roll-up), used to merge a page's two disjoint groups back into listing order.
fn sort_key(e: &Entry) -> &str {
    match e {
        Entry::Item(k) => k,
        Entry::CommonPrefix(cp) => cp,
    }
}

/// Keys drawn from a tiny alphabet plus the candidate delimiter `/`, so prefixes and delimiters
/// collide often enough to exercise the roll-up and seek paths. Length 1..=6 keeps the
/// keyspace dense; `ObjectKey::parse` accepts all of these (it only forbids empty/NUL/overlong).
fn key_strategy() -> impl Strategy<Value = String> {
    vec(prop::sample::select(vec!['a', 'b', '/', '1']), 1..=6)
        .prop_map(|cs| cs.into_iter().collect::<String>())
}

proptest! {
    // A modest case count: each case bootstraps a fresh in-memory store and inserts up to ~30
    // keys, so the suite stays well under a second while still crossing many page boundaries.
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    #[test]
    fn list_current_matches_oracle_across_pages(
        keys in btree_set(key_strategy(), 0..30),
        // The prefix is itself drawn from the same alphabet (often empty) so it frequently
        // matches real keys; the delimiter is `/` or absent; max-keys spans tiny pages (forcing
        // many boundaries) up to a single-page ceiling.
        prefix in prop::option::weighted(0.7, key_strategy()),
        delimiter in prop::option::weighted(0.6, Just("/".to_owned())),
        max_keys in 1u32..=8,
    ) {
        let keys: Vec<String> = keys.into_iter().collect();
        let prefix = prefix.unwrap_or_default();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let store = cairn_meta::open_in_memory().unwrap();
            let bucket = BucketName::parse("bkt").unwrap();
            for k in &keys {
                store.submit(put(row(&bucket, k))).await.unwrap();
            }

            let want = oracle_entries(&keys, &prefix, delimiter.as_deref());
            let (got, pages) =
                drain_store(&store, &bucket, &prefix, delimiter.as_deref(), max_keys).await;

            prop_assert_eq!(
                &got,
                &want,
                "drained listing disagreed with oracle: prefix={:?} delimiter={:?} max_keys={} keys={:?}",
                prefix,
                delimiter,
                max_keys,
                keys
            );

            // Sanity: a result longer than one page's ceiling must have actually paginated.
            if want.len() > max_keys as usize {
                prop_assert!(pages > 1, "expected multiple pages for {} entries", want.len());
            }
            Ok(())
        })?;
    }
}
