//! Replication-configuration types and the S3 `<ReplicationConfiguration>` XML parser.
//!
//! A bucket's replication configuration (ARCH 20.1) carries an IAM `<Role>` and a list of
//! [`ReplicationRule`]s. Each rule has an identifier, an enabled/disabled status, an optional
//! prefix [`Filter`] selecting which keys it applies to, and a [`Destination`] naming the
//! remote bucket (by ARN). The server consumes the typed [`ReplicationConfig`] to decide
//! enqueue-on-write (a current-version write under a matching, enabled rule's prefix enqueues
//! an outbox entry) and to construct a sink for each destination.
//!
//! The parser is a total function over an arbitrary byte slice: any malformed input — invalid
//! UTF-8, unbalanced tags, an unrecognized status — folds to [`Error::MalformedXml`], and the
//! parser never panics. It drives quick-xml through a small SAX layer ([`drive`]) that tracks
//! element depth so a body which reaches EOF with an element still open is rejected, mirroring
//! the lifecycle parser and the codec in `cairn-xml`.

use cairn_types::Error;
use cairn_types::bucket::VersioningState;
use quick_xml::Reader;
use quick_xml::events::Event;

/// A parsed bucket replication configuration: the role assumed to replicate and the rules.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicationConfig {
    /// The IAM role ARN replication assumes (`<Role>`). Retained for fidelity; Cairn's sink
    /// authenticates with the destination's own access key rather than assuming a role.
    pub role: String,
    /// The replication rules, in document order.
    pub rules: Vec<ReplicationRule>,
}

impl ReplicationConfig {
    /// The enabled rule that applies to `key` by **prefix only**, if any. Among prefix matches the
    /// highest [`priority`](ReplicationRule::priority) wins; ties fall back to document order (the
    /// first such rule). This is the key-only fast path; use
    /// [`matching_rule_for`](Self::matching_rule_for) when the object's tags are known so tag
    /// predicates are honoured.
    #[must_use]
    pub fn matching_rule(&self, key: &str) -> Option<&ReplicationRule> {
        // Highest priority wins; on a tie the earlier (document-order) rule is kept, so a strict
        // `>` comparison never displaces an equal-priority earlier match.
        self.rules
            .iter()
            .filter(|r| r.enabled && r.filter.matches_prefix(key))
            .reduce(|best, r| if r.priority > best.priority { r } else { best })
    }

    /// The enabled rule that applies to `key` carrying `tags`, honouring both the prefix and the
    /// tag predicates. Among matches the highest [`priority`](ReplicationRule::priority) wins; ties
    /// fall back to document order.
    #[must_use]
    pub fn matching_rule_for(
        &self,
        key: &str,
        tags: &[(String, String)],
    ) -> Option<&ReplicationRule> {
        self.rules
            .iter()
            .filter(|r| r.enabled && r.filter.matches(key, tags))
            .reduce(|best, r| if r.priority > best.priority { r } else { best })
    }

    /// Whether a write to `key` should enqueue a replication entry under this configuration: it
    /// matches an enabled rule's prefix filter.
    #[must_use]
    pub fn replicates(&self, key: &str) -> bool {
        self.matching_rule(key).is_some()
    }

    /// Validate this configuration for a source bucket in the given versioning state. S3
    /// requires the source bucket be versioning-**enabled** before replication can be turned on
    /// (a delete marker or new version must be identifiable); the caller passes the source
    /// bucket's [`VersioningState`].
    ///
    /// # Errors
    /// Returns [`Error::InvalidRequest`] if the source bucket is not versioning-enabled, if no
    /// rule is present, if a rule names no destination bucket, or if the role is empty.
    pub fn validate(&self, source_versioning: VersioningState) -> Result<(), Error> {
        if source_versioning != VersioningState::Enabled {
            return Err(Error::InvalidRequest(
                "replication requires the source bucket to be versioning-enabled".to_owned(),
            ));
        }
        if self.role.trim().is_empty() {
            return Err(Error::InvalidRequest(
                "replication configuration is missing a role".to_owned(),
            ));
        }
        if self.rules.is_empty() {
            return Err(Error::InvalidRequest(
                "replication configuration has no rules".to_owned(),
            ));
        }
        for rule in &self.rules {
            if rule.destination.bucket().is_none() {
                return Err(Error::InvalidRequest(format!(
                    "replication rule {:?} names no destination bucket",
                    rule.id
                )));
            }
        }
        Ok(())
    }
}

/// A replication rule: an identifier, a status, a prefix filter, and a destination.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicationRule {
    /// The rule identifier (`<ID>`); empty if the configuration omitted it.
    pub id: String,
    /// Whether the rule is enabled (`<Status>Enabled</Status>`). A disabled rule is parsed and
    /// retained but never selects a key for replication.
    pub enabled: bool,
    /// The key selector. An empty filter (the default) applies to every key in the bucket.
    pub filter: Filter,
    /// The destination this rule replicates to.
    pub destination: Destination,
    /// The rule's dispatch priority (`<Priority>`). When several rules match a key, the highest
    /// priority wins; ties fall back to document order. Defaults to `0`. Carried onto each
    /// enqueued [`OutboxEntry`](cairn_types::meta::OutboxEntry) so the outbox drains hot rules
    /// first.
    pub priority: i64,
    /// The remote target ARN this rule ships to (`<Destination><Bucket>arn:cairn:…</Bucket>`),
    /// when the destination names a [`RemoteTarget`](crate::RemoteTarget) ARN rather than a plain
    /// S3 bucket ARN. `None` for the legacy fixed-destination form.
    pub target_arn: Option<String>,
    /// Whether delete markers are replicated (`<DeleteMarkerReplication><Status>Enabled`).
    pub delete_marker_replication: bool,
    /// Whether pre-existing objects are backfilled (`<ExistingObjectReplication><Status>Enabled`).
    pub existing_object_replication: bool,
}

/// The selector that scopes a rule to a subset of a bucket's keys. An empty prefix matches
/// every key. Cairn supports both the prefix filter (S3's `<Prefix>` and `<Filter><Prefix>`
/// forms) and tag predicates (S3's `<Filter><Tag>`/`<Filter><And><Tag>` forms): a key matches
/// when it begins with the prefix **and** carries every required tag (key=value).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Filter {
    /// Restrict to keys beginning with this prefix. `None`/empty matches every key.
    pub prefix: Option<String>,
    /// Required object tags, as `(key, value)` predicates. A key matches only if its tag set
    /// contains every one of these pairs. An empty list imposes no tag constraint.
    pub tags: Vec<(String, String)>,
}

impl Filter {
    /// Whether a key matches this filter's **prefix only** (ignoring any tag predicates). This is
    /// the entry point for callers that have only the key in hand (the enqueue-on-write fast path
    /// where object tags are applied separately); use [`matches`](Self::matches) when the object's
    /// tags are available.
    #[must_use]
    pub fn matches_prefix(&self, key: &str) -> bool {
        match &self.prefix {
            Some(p) => key.starts_with(p.as_str()),
            None => true,
        }
    }

    /// Whether a key with the given object tags matches this filter: the prefix matches **and**
    /// every required tag predicate is satisfied by `tags`.
    #[must_use]
    pub fn matches(&self, key: &str, tags: &[(String, String)]) -> bool {
        if !self.matches_prefix(key) {
            return false;
        }
        self.tags
            .iter()
            .all(|(k, v)| tags.iter().any(|(tk, tv)| tk == k && tv == v))
    }
}

/// A replication destination: the remote bucket, given as an S3 ARN (`arn:aws:s3:::bucket`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Destination {
    /// The raw destination bucket ARN (`<Destination><Bucket>`).
    pub bucket_arn: String,
}

impl Destination {
    /// The bare destination bucket name, stripped of the `arn:aws:s3:::` prefix if present.
    /// Returns `None` if the ARN is empty.
    #[must_use]
    pub fn bucket(&self) -> Option<&str> {
        let raw = self.bucket_arn.trim();
        let name = raw.strip_prefix("arn:aws:s3:::").unwrap_or(raw);
        if name.is_empty() { None } else { Some(name) }
    }
}

/// Parse an S3 `<ReplicationConfiguration>` body into a typed [`ReplicationConfig`].
///
/// The document carries a single `<Role>` and one or more `<Rule>`s. Each rule's `<Status>` is
/// `Enabled` or `Disabled` (any other value is malformed); its `<Prefix>` (either directly
/// under `<Rule>` or nested in `<Filter>`) scopes the rule; and its `<Destination><Bucket>`
/// names the target. An empty configuration parses to an empty rule list.
///
/// This is the wire parser only; it performs no semantic validation (versioning state, a
/// present destination, a non-empty role). Use [`ReplicationConfig::validate`] for that.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed XML or a `<Status>` is
/// unrecognized.
pub fn parse_replication(body: &[u8]) -> Result<ReplicationConfig, Error> {
    let mut config = ReplicationConfig::default();

    // Per-rule accumulator state.
    let mut in_rule = false;
    let mut rule = ReplicationRule::default();
    // The in-flight `<Tag>` being read inside a `<Filter>` (`<Key>`/`<Value>` pair).
    let mut tag_key: Option<String> = None;
    let mut tag_value: Option<String> = None;

    // A stack of currently-open element local names, so a parser can disambiguate where a text
    // leaf belongs (e.g. `Bucket` under `Destination` vs a stray element).
    let mut stack: Vec<Vec<u8>> = Vec::new();

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                if name.as_slice() == b"Rule" {
                    in_rule = true;
                    rule = ReplicationRule::default();
                } else if name.as_slice() == b"Tag" {
                    tag_key = None;
                    tag_value = None;
                }
                stack.push(name);
            }
            Sax::Text(text) => {
                let Some(field) = stack.last() else {
                    return Ok(());
                };
                let parent = stack.get(stack.len().wrapping_sub(2)).map(Vec::as_slice);
                match field.as_slice() {
                    // `<Role>` lives directly under `<ReplicationConfiguration>`.
                    b"Role" if !in_rule => config.role = text.trim().to_owned(),
                    _ if !in_rule => {}
                    b"ID" => rule.id = text.trim().to_owned(),
                    // `<Status>` is overloaded: directly under `<Rule>` it is the rule's
                    // enabled/disabled status, but the same element nested under
                    // `<DeleteMarkerReplication>` / `<ExistingObjectReplication>` toggles those
                    // sub-features. Disambiguate by the parent element.
                    b"Status" if parent == Some(b"DeleteMarkerReplication") => {
                        rule.delete_marker_replication = match text.trim() {
                            "Enabled" => true,
                            "Disabled" => false,
                            _ => return Err(malformed()),
                        };
                    }
                    b"Status" if parent == Some(b"ExistingObjectReplication") => {
                        rule.existing_object_replication = match text.trim() {
                            "Enabled" => true,
                            "Disabled" => false,
                            _ => return Err(malformed()),
                        };
                    }
                    b"Status" => {
                        rule.enabled = match text.trim() {
                            "Enabled" => true,
                            "Disabled" => false,
                            _ => return Err(malformed()),
                        };
                    }
                    b"Priority" => {
                        rule.priority = text.trim().parse().map_err(|_| malformed())?;
                    }
                    b"Prefix" => {
                        // `<Prefix>` may appear directly under `<Rule>` or nested in `<Filter>`
                        // / `<And>`; treat all positions as the rule prefix.
                        rule.filter.prefix = Some(text.into_owned());
                    }
                    // `<Key>`/`<Value>` inside a `<Tag>` accumulate one tag predicate.
                    b"Key" if parent == Some(b"Tag") => {
                        tag_key = Some(text.trim().to_owned());
                    }
                    b"Value" if parent == Some(b"Tag") => {
                        tag_value = Some(text.trim().to_owned());
                    }
                    // `<Bucket>` is meaningful only inside `<Destination>`.
                    b"Bucket" if parent == Some(b"Destination") => {
                        rule.destination.bucket_arn = text.trim().to_owned();
                    }
                    _ => {}
                }
            }
            Sax::Close(name) => {
                let popped = stack.pop();
                debug_assert!(popped.as_deref() == Some(name.as_slice()) || popped.is_none());
                match name.as_slice() {
                    b"Tag" if in_rule => {
                        // A complete `<Tag>` commits one (key, value) predicate; an incomplete tag
                        // (missing key or value) is ignored rather than rejected.
                        if let (Some(k), Some(v)) = (tag_key.take(), tag_value.take()) {
                            rule.filter.tags.push((k, v));
                        }
                    }
                    b"Rule" if in_rule => {
                        // MinIO-style rules carry a remote-target ARN in `<Destination><Bucket>`;
                        // surface it as `target_arn` while leaving the raw ARN on the destination.
                        if let Some(arn) = target_arn_of(&rule.destination.bucket_arn) {
                            rule.target_arn = Some(arn);
                        }
                        config.rules.push(std::mem::take(&mut rule));
                        in_rule = false;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    })?;

    Ok(config)
}

// ===========================================================================================
// SAX driver (mirrors the well-formedness/balance discipline of cairn-lifecycle / cairn-xml)
// ===========================================================================================

/// Map any quick-xml failure into the canonical malformed-XML error.
fn malformed() -> Error {
    Error::MalformedXml
}

/// Recognise a MinIO-style remote-target ARN in a `<Destination><Bucket>` value: a Cairn
/// replication-target ARN (`arn:cairn:replication:…`) is returned for use as the rule's
/// `target_arn`; a plain S3 bucket ARN or bare name is not a target reference and yields `None`.
fn target_arn_of(bucket_arn: &str) -> Option<String> {
    let raw = bucket_arn.trim();
    if raw.starts_with("arn:cairn:replication:") {
        Some(raw.to_owned())
    } else {
        None
    }
}

/// One decoded SAX event handed to the parser callback.
enum Sax<'a> {
    /// An opening tag with its local name (namespace prefix stripped).
    Open(Vec<u8>),
    /// Decoded, entity-unescaped text content.
    Text(std::borrow::Cow<'a, str>),
    /// A closing tag with its local name.
    Close(Vec<u8>),
}

/// Drive an XML body through a callback, validating well-formedness and element balance. A
/// self-closing tag surfaces as an `Open` immediately followed by a `Close`. A body that ends
/// with any element still open is rejected as malformed.
fn drive<F>(body: &[u8], mut on_event: F) -> Result<(), Error>
where
    F: FnMut(Sax<'_>) -> Result<(), Error>,
{
    let s = std::str::from_utf8(body).map_err(|_| malformed())?;
    let mut reader = Reader::from_str(s);
    let cfg = reader.config_mut();
    cfg.trim_text(true);

    let mut depth: u32 = 0;
    loop {
        match reader.read_event().map_err(|_| malformed())? {
            Event::Start(e) => {
                depth += 1;
                on_event(Sax::Open(local(e.name())))?;
            }
            Event::Empty(e) => {
                let name = local(e.name());
                on_event(Sax::Open(name.clone()))?;
                on_event(Sax::Close(name))?;
            }
            Event::Text(t) => {
                let text = t.unescape().map_err(|_| malformed())?;
                on_event(Sax::Text(text))?;
            }
            Event::CData(t) => {
                let text = t.decode().map_err(|_| malformed())?;
                on_event(Sax::Text(text))?;
            }
            Event::End(e) => {
                depth = depth.checked_sub(1).ok_or_else(malformed)?;
                on_event(Sax::Close(local(e.name())))?;
            }
            Event::Eof => break,
            _ => {}
        }
    }
    if depth != 0 {
        return Err(malformed());
    }
    Ok(())
}

/// The local element name of an event, as owned bytes (namespace prefix stripped).
fn local(name: quick_xml::name::QName<'_>) -> Vec<u8> {
    quick_xml::name::LocalName::from(name).as_ref().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPRESENTATIVE: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?>
<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role>arn:aws:iam::123456789012:role/replication-role</Role>
  <Rule>
    <ID>logs-rule</ID>
    <Status>Enabled</Status>
    <Prefix>logs/</Prefix>
    <Destination>
      <Bucket>arn:aws:s3:::dest-bucket</Bucket>
    </Destination>
  </Rule>
  <Rule>
    <ID>archive-rule</ID>
    <Status>Disabled</Status>
    <Filter>
      <Prefix>archive/</Prefix>
    </Filter>
    <Destination>
      <Bucket>arn:aws:s3:::cold-bucket</Bucket>
    </Destination>
  </Rule>
</ReplicationConfiguration>"#;

    #[test]
    fn parses_representative_config() {
        let cfg = parse_replication(REPRESENTATIVE).unwrap();
        assert_eq!(cfg.role, "arn:aws:iam::123456789012:role/replication-role");
        assert_eq!(cfg.rules.len(), 2);

        let r0 = &cfg.rules[0];
        assert_eq!(r0.id, "logs-rule");
        assert!(r0.enabled);
        assert_eq!(r0.filter.prefix.as_deref(), Some("logs/"));
        assert_eq!(r0.destination.bucket(), Some("dest-bucket"));

        let r1 = &cfg.rules[1];
        assert_eq!(r1.id, "archive-rule");
        assert!(!r1.enabled);
        // The prefix is read whether nested under `<Filter>` or not.
        assert_eq!(r1.filter.prefix.as_deref(), Some("archive/"));
        assert_eq!(r1.destination.bucket(), Some("cold-bucket"));
    }

    #[test]
    fn matching_rule_picks_first_enabled_prefix_match() {
        let cfg = parse_replication(REPRESENTATIVE).unwrap();
        // The enabled `logs/` rule matches; the disabled `archive/` rule never matches.
        assert!(cfg.replicates("logs/2026/app.log"));
        assert_eq!(
            cfg.matching_rule("logs/x").map(|r| r.id.as_str()),
            Some("logs-rule")
        );
        assert!(!cfg.replicates("archive/2026/old.tar"));
        assert!(!cfg.replicates("photos/cat.jpg"));
    }

    #[test]
    fn empty_prefix_matches_all_keys() {
        let xml = br#"<ReplicationConfiguration>
            <Role>r</Role>
            <Rule><ID>all</ID><Status>Enabled</Status>
              <Destination><Bucket>arn:aws:s3:::d</Bucket></Destination></Rule>
        </ReplicationConfiguration>"#;
        let cfg = parse_replication(xml).unwrap();
        assert_eq!(cfg.rules.len(), 1);
        assert!(cfg.rules[0].filter.prefix.is_none());
        assert!(cfg.replicates("anything/at/all"));
    }

    #[test]
    fn empty_configuration_parses_to_no_rules() {
        let cfg =
            parse_replication(b"<ReplicationConfiguration></ReplicationConfiguration>").unwrap();
        assert!(cfg.rules.is_empty());
        assert!(cfg.role.is_empty());
    }

    #[test]
    fn rejects_unbalanced_xml() {
        let err = parse_replication(b"<ReplicationConfiguration><Rule>").unwrap_err();
        assert!(matches!(err, Error::MalformedXml));
    }

    #[test]
    fn rejects_invalid_utf8() {
        let err = parse_replication(&[0xff, 0xfe, 0x00]).unwrap_err();
        assert!(matches!(err, Error::MalformedXml));
    }

    #[test]
    fn rejects_unknown_status() {
        let xml = br#"<ReplicationConfiguration><Role>r</Role>
            <Rule><Status>Paused</Status>
              <Destination><Bucket>arn:aws:s3:::d</Bucket></Destination></Rule>
        </ReplicationConfiguration>"#;
        let err = parse_replication(xml).unwrap_err();
        assert!(matches!(err, Error::MalformedXml));
    }

    #[test]
    fn validate_requires_versioning_enabled() {
        let cfg = parse_replication(REPRESENTATIVE).unwrap();
        assert!(cfg.validate(VersioningState::Enabled).is_ok());

        let err = cfg.validate(VersioningState::Suspended).unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)));
        let err = cfg.validate(VersioningState::Unversioned).unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)));
    }

    #[test]
    fn validate_rejects_missing_role_rules_or_destination() {
        // No role.
        let no_role = ReplicationConfig {
            role: String::new(),
            rules: vec![ReplicationRule {
                id: "r".to_owned(),
                enabled: true,
                filter: Filter::default(),
                destination: Destination {
                    bucket_arn: "arn:aws:s3:::d".to_owned(),
                },
                ..ReplicationRule::default()
            }],
        };
        assert!(matches!(
            no_role.validate(VersioningState::Enabled),
            Err(Error::InvalidRequest(_))
        ));

        // No rules.
        let no_rules = ReplicationConfig {
            role: "r".to_owned(),
            rules: Vec::new(),
        };
        assert!(matches!(
            no_rules.validate(VersioningState::Enabled),
            Err(Error::InvalidRequest(_))
        ));

        // Rule with an empty destination.
        let no_dest = ReplicationConfig {
            role: "r".to_owned(),
            rules: vec![ReplicationRule {
                id: "r".to_owned(),
                enabled: true,
                filter: Filter::default(),
                destination: Destination::default(),
                ..ReplicationRule::default()
            }],
        };
        assert!(matches!(
            no_dest.validate(VersioningState::Enabled),
            Err(Error::InvalidRequest(_))
        ));
    }

    const MINIO_STYLE: &[u8] = br#"<ReplicationConfiguration>
      <Role></Role>
      <Rule>
        <ID>to-remote</ID>
        <Status>Enabled</Status>
        <Priority>5</Priority>
        <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
        <ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>
        <Filter>
          <And>
            <Prefix>data/</Prefix>
            <Tag><Key>replicate</Key><Value>yes</Value></Tag>
            <Tag><Key>tier</Key><Value>gold</Value></Tag>
          </And>
        </Filter>
        <Destination>
          <Bucket>arn:cairn:replication:us-west-2:abc123:mirror</Bucket>
        </Destination>
      </Rule>
    </ReplicationConfiguration>"#;

    #[test]
    fn parses_minio_style_rule_fields() {
        let cfg = parse_replication(MINIO_STYLE).unwrap();
        assert_eq!(cfg.rules.len(), 1);
        let r = &cfg.rules[0];
        assert_eq!(r.id, "to-remote");
        assert!(r.enabled);
        assert_eq!(r.priority, 5);
        assert!(r.delete_marker_replication);
        assert!(r.existing_object_replication);
        assert_eq!(r.filter.prefix.as_deref(), Some("data/"));
        assert_eq!(
            r.filter.tags,
            vec![
                ("replicate".to_owned(), "yes".to_owned()),
                ("tier".to_owned(), "gold".to_owned()),
            ]
        );
        // The cairn replication ARN surfaces as the rule's target_arn.
        assert_eq!(
            r.target_arn.as_deref(),
            Some("arn:cairn:replication:us-west-2:abc123:mirror")
        );
    }

    #[test]
    fn tag_filter_is_honoured_by_matches() {
        let cfg = parse_replication(MINIO_STYLE).unwrap();
        let f = &cfg.rules[0].filter;
        // Prefix-only match ignores tags.
        assert!(f.matches_prefix("data/x"));
        // Full match requires the prefix AND every tag predicate.
        let all_tags = [
            ("replicate".to_owned(), "yes".to_owned()),
            ("tier".to_owned(), "gold".to_owned()),
            ("extra".to_owned(), "ok".to_owned()),
        ];
        assert!(f.matches("data/x", &all_tags));
        // Missing a required tag -> no match.
        let missing = [("replicate".to_owned(), "yes".to_owned())];
        assert!(!f.matches("data/x", &missing));
        // Wrong prefix -> no match even with all tags.
        assert!(!f.matches("other/x", &all_tags));
    }

    #[test]
    fn matching_rule_breaks_ties_by_priority() {
        let xml = br#"<ReplicationConfiguration><Role>r</Role>
          <Rule><ID>low</ID><Status>Enabled</Status><Priority>1</Priority>
            <Destination><Bucket>arn:aws:s3:::a</Bucket></Destination></Rule>
          <Rule><ID>high</ID><Status>Enabled</Status><Priority>9</Priority>
            <Destination><Bucket>arn:aws:s3:::b</Bucket></Destination></Rule>
        </ReplicationConfiguration>"#;
        let cfg = parse_replication(xml).unwrap();
        // Both match every key; the higher priority wins.
        assert_eq!(cfg.matching_rule("k").map(|r| r.id.as_str()), Some("high"));
    }

    #[test]
    fn equal_priority_keeps_document_order() {
        // Default priority 0 for both: the first (document order) match wins.
        let cfg = parse_replication(REPRESENTATIVE).unwrap();
        assert_eq!(
            cfg.matching_rule("logs/x").map(|r| r.id.as_str()),
            Some("logs-rule")
        );
    }

    #[test]
    fn rejects_bad_priority() {
        let xml = br#"<ReplicationConfiguration><Role>r</Role>
          <Rule><Status>Enabled</Status><Priority>not-a-number</Priority>
            <Destination><Bucket>arn:aws:s3:::d</Bucket></Destination></Rule>
        </ReplicationConfiguration>"#;
        assert!(matches!(
            parse_replication(xml).unwrap_err(),
            Error::MalformedXml
        ));
    }

    #[test]
    fn plain_s3_destination_has_no_target_arn() {
        let cfg = parse_replication(REPRESENTATIVE).unwrap();
        assert!(cfg.rules[0].target_arn.is_none());
        assert_eq!(cfg.rules[0].destination.bucket(), Some("dest-bucket"));
    }

    #[test]
    fn destination_strips_arn_prefix() {
        let d = Destination {
            bucket_arn: "arn:aws:s3:::my-bucket".to_owned(),
        };
        assert_eq!(d.bucket(), Some("my-bucket"));
        // A bare name (no ARN prefix) passes through.
        let bare = Destination {
            bucket_arn: "plain-bucket".to_owned(),
        };
        assert_eq!(bare.bucket(), Some("plain-bucket"));
        assert_eq!(Destination::default().bucket(), None);
    }
}
