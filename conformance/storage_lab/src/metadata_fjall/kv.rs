//! Ordered encoding and bounded staged transactions for the isolated Fjall candidate.
use cairn_types::MetaError;
use fjall::{Readable, SingleWriterTxKeyspace};
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, ops::Bound};

pub const PAGE: usize = 256;
const MAX_VALUE: usize = 32 * 1024;
const MAX_EDITS: usize = 16_384;
const MAX_OVERLAY: usize = 16 * 1024 * 1024;
pub type Pair = (Vec<u8>, Vec<u8>);

pub fn error(value: impl std::fmt::Display) -> MetaError {
    MetaError::Engine(value.to_string())
}

/// Escaped, terminated byte strings preserve lexical tuple order, including embedded NUL.
pub fn component(out: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            out.extend_from_slice(&[0, 255]);
        } else {
            out.push(*byte);
        }
    }
    out.extend_from_slice(&[0, 0]);
}
pub fn key(table: u8, fields: &[&str]) -> Vec<u8> {
    let mut output = vec![table];
    for field in fields {
        component(&mut output, field.as_bytes());
    }
    output
}
pub fn raw_prefix(table: u8, fields: &[&str], partial: &str) -> Vec<u8> {
    let mut output = key(table, fields);
    component(&mut output, partial.as_bytes());
    output.truncate(output.len() - 2);
    output
}
pub fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(byte) = end.pop() {
        if byte != 255 {
            end.push(byte + 1);
            return Some(end);
        }
    }
    None
}
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, MetaError> {
    let data = serde_json::to_vec(value).map_err(error)?;
    if data.len() > MAX_VALUE {
        return Err(error("candidate value exceeds its bounded codec"));
    }
    Ok(data)
}
pub fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T, MetaError> {
    if data.len() > MAX_VALUE {
        return Err(error("oversized persisted candidate value"));
    }
    serde_json::from_slice(data).map_err(error)
}

pub trait View {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError>;
    /// Return at most PAGE rows strictly after a cursor, from one prefix range.
    fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Pair>, MetaError>;
}
pub fn get<T: DeserializeOwned>(view: &dyn View, key: &[u8]) -> Result<Option<T>, MetaError> {
    view.get(key)?.map(|data| decode(&data)).transpose()
}
pub fn exists(view: &dyn View, prefix: &[u8]) -> Result<bool, MetaError> {
    Ok(!view.scan(prefix, None, 1)?.is_empty())
}

pub struct Native<'a, R: Readable> {
    pub transaction: &'a R,
    pub tree: &'a SingleWriterTxKeyspace,
}
impl<R: Readable> View for Native<'_, R> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        self.transaction
            .get(self.tree, key)
            .map(|v| v.map(|v| v.to_vec()))
            .map_err(error)
    }
    fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Pair>, MetaError> {
        if after
            .is_some_and(|after| successor(prefix).is_some_and(|upper| after >= upper.as_slice()))
        {
            return Ok(Vec::new());
        }
        if limit > PAGE {
            return Err(error("candidate read page exceeds bound"));
        }
        let lower = after.filter(|cursor| *cursor >= prefix).map_or_else(
            || Bound::Included(prefix.to_vec()),
            |cursor| Bound::Excluded(cursor.to_vec()),
        );
        let upper = successor(prefix).map_or(Bound::Unbounded, Bound::Excluded);
        self.transaction
            .range(self.tree, (lower, upper))
            .take(limit)
            .map(|entry| {
                entry
                    .into_inner()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .map_err(error)
            })
            .collect()
    }
}

/// A member never changes its parent's write set until all validation has succeeded.
/// Nested member/batch overlays remain bounded; scans merge one PAGE-sized base page at a time.
pub struct Overlay<'a> {
    base: &'a dyn View,
    edits: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    bytes: usize,
}
impl<'a> Overlay<'a> {
    pub fn new(base: &'a dyn View) -> Self {
        Self {
            base,
            edits: BTreeMap::new(),
            bytes: 0,
        }
    }
    fn charge(key: &[u8], value: Option<&Vec<u8>>) -> usize {
        key.len() + value.map_or(0, Vec::len) + 128
    }
    pub fn set(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<(), MetaError> {
        if key.len() > 8192 || value.as_ref().is_some_and(|v| v.len() > MAX_VALUE) {
            return Err(error("candidate key/value exceeds declared bound"));
        }
        let old = self
            .edits
            .get(&key)
            .map_or(0, |old| Self::charge(&key, old.as_ref()));
        let next = self.bytes - old + Self::charge(&key, value.as_ref());
        if next > MAX_OVERLAY || (!self.edits.contains_key(&key) && self.edits.len() == MAX_EDITS) {
            return Err(error("candidate staged mutation exceeds admission"));
        }
        self.edits.insert(key, value);
        self.bytes = next;
        Ok(())
    }
    pub fn put<T: Serialize>(&mut self, key: Vec<u8>, value: &T) -> Result<(), MetaError> {
        self.set(key, Some(encode(value)?))
    }
    pub fn remove(&mut self, key: Vec<u8>) -> Result<(), MetaError> {
        self.set(key, None)
    }
    pub fn apply_member(
        &mut self,
        edits: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> Result<(), MetaError> {
        let mut bytes = self.bytes;
        let mut keys = self.edits.len();
        for (key, value) in &edits {
            if let Some(old) = self.edits.get(key) {
                bytes -= Self::charge(key, old.as_ref());
            } else {
                keys += 1;
            }
            bytes += Self::charge(key, value.as_ref());
        }
        if bytes > MAX_OVERLAY || keys > MAX_EDITS {
            return Err(error("candidate batch staging admission exceeded"));
        }
        self.edits.extend(edits);
        self.bytes = bytes;
        Ok(())
    }
    pub fn into_edits(self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        self.edits
    }
}
impl View for Overlay<'_> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        self.edits
            .get(key)
            .cloned()
            .map_or_else(|| self.base.get(key), Ok)
    }
    fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Pair>, MetaError> {
        if after
            .is_some_and(|after| successor(prefix).is_some_and(|upper| after >= upper.as_slice()))
        {
            return Ok(Vec::new());
        }
        if limit > PAGE {
            return Err(error("candidate overlay read page exceeds bound"));
        }
        let lower = after.filter(|cursor| *cursor >= prefix).map_or_else(
            || Bound::Included(prefix.to_vec()),
            |cursor| Bound::Excluded(cursor.to_vec()),
        );
        let upper = successor(prefix).map_or(Bound::Unbounded, Bound::Excluded);
        let mut changed = self.edits.range((lower, upper)).peekable();
        let mut cursor = after.map(<[u8]>::to_vec);
        let mut base = self
            .base
            .scan(prefix, cursor.as_deref(), PAGE)?
            .into_iter()
            .peekable();
        let mut exhausted = base.len() < PAGE;
        let mut output = Vec::with_capacity(limit);
        while output.len() < limit {
            if base.peek().is_none() && !exhausted {
                let next = self.base.scan(prefix, cursor.as_deref(), PAGE)?;
                exhausted = next.len() < PAGE;
                base = next.into_iter().peekable();
            }
            let use_change = match (changed.peek(), base.peek()) {
                (Some((key, _)), Some((base_key, _))) => *key <= base_key,
                (Some(_), None) => true,
                _ => false,
            };
            if use_change {
                let (key, value) = changed.next().expect("peeked change");
                if base.peek().is_some_and(|(base_key, _)| base_key == key) {
                    let (base_key, _) = base.next().expect("peeked base");
                    cursor = Some(base_key);
                }
                if let Some(value) = value {
                    output.push((key.clone(), value.clone()));
                }
            } else if let Some((key, value)) = base.next() {
                cursor = Some(key.clone());
                output.push((key, value));
            } else {
                break;
            }
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};

    #[test]
    fn byte_tuple_order_and_prefix_boundaries_are_exact() {
        let values = ["", "\0", "\0a", "a", "a\0", "aa", "b", "é", "雪"];
        let mut encoded: Vec<_> = values.iter().map(|s| (key(1, &[s, "tail"]), *s)).collect();
        encoded.sort();
        assert_eq!(encoded.iter().map(|(_, s)| *s).collect::<Vec<_>>(), values);
        let prefix = raw_prefix(1, &["bucket"], "a\0");
        let candidate = key(1, &["bucket", "a\0x", "v"]);
        assert!(candidate.starts_with(&prefix));
        assert!(candidate < successor(&prefix).unwrap());
        assert!(!key(1, &["bucket", "aa", "v"]).starts_with(&prefix));
        assert_eq!(successor(&[255, 255]), None);
    }

    #[test]
    fn late_member_failure_preserves_prior_members_and_fresh_full_reopen() {
        let root = tempfile::tempdir().unwrap();
        {
            let db = SingleWriterTxDatabase::builder(root.path()).open().unwrap();
            let tree = db.keyspace("rows", KeyspaceCreateOptions::default).unwrap();
            let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
            let native = Native {
                transaction: &tx,
                tree: &tree,
            };
            let mut batch = Overlay::new(&native);
            let accepted = {
                let mut member = Overlay::new(&batch);
                member.put(key(1, &["first"]), &7_u64).unwrap();
                member.into_edits()
            };
            batch.apply_member(accepted).unwrap();
            {
                let mut rejected = Overlay::new(&batch);
                assert_eq!(get::<u64>(&rejected, &key(1, &["first"])).unwrap(), Some(7));
                rejected.put(key(1, &["first"]), &99_u64).unwrap();
                rejected.put(key(2, &["outbox"]), &1_u64).unwrap();
                // A late validation/constraint error discards this entire member.
            }
            assert_eq!(get::<u64>(&batch, &key(1, &["first"])).unwrap(), Some(7));
            assert!(!exists(&batch, &[2]).unwrap());
            for (key, value) in batch.into_edits() {
                if let Some(value) = value {
                    tx.insert(&tree, key, value);
                } else {
                    tx.remove(&tree, key);
                }
            }
            tx.commit().unwrap();
            db.persist(PersistMode::SyncAll).unwrap();
        }
        let db = SingleWriterTxDatabase::builder(root.path()).open().unwrap();
        let tree = db.keyspace("rows", KeyspaceCreateOptions::default).unwrap();
        let snap = db.read_tx();
        let view = Native {
            transaction: &snap,
            tree: &tree,
        };
        assert_eq!(get::<u64>(&view, &key(1, &["first"])).unwrap(), Some(7));
        assert!(!exists(&view, &[2]).unwrap());
    }

    #[test]
    fn overlay_pages_merge_deletions_replacements_and_insertions_without_gaps() {
        let root = tempfile::tempdir().unwrap();
        let db = SingleWriterTxDatabase::builder(root.path()).open().unwrap();
        let tree = db.keyspace("rows", KeyspaceCreateOptions::default).unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        for i in 0..700 {
            tx.insert(&tree, format!("p{i:04}"), "old");
        }
        tx.commit().unwrap();
        let snap = db.read_tx();
        let native = Native {
            transaction: &snap,
            tree: &tree,
        };
        let mut overlay = Overlay::new(&native);
        for i in 0..500 {
            overlay.remove(format!("p{i:04}").into_bytes()).unwrap();
        }
        overlay
            .set(b"p0600".to_vec(), Some(b"new".to_vec()))
            .unwrap();
        overlay
            .set(b"p0600x".to_vec(), Some(b"extra".to_vec()))
            .unwrap();
        let first = overlay.scan(b"p", None, 128).unwrap();
        let second = overlay
            .scan(b"p", Some(&first.last().unwrap().0), 128)
            .unwrap();
        let combined: Vec<_> = first.into_iter().chain(second).collect();
        assert_eq!(combined.len(), 201);
        assert!(combined.windows(2).all(|w| w[0].0 < w[1].0));
        assert_eq!(combined[0].0, b"p0500");
        assert_eq!(
            combined.iter().find(|(k, _)| k == b"p0600").unwrap().1,
            b"new"
        );
        assert_eq!(
            combined.iter().find(|(k, _)| k == b"p0600x").unwrap().1,
            b"extra"
        );
    }
}
