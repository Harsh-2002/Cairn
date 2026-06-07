//! Replication-configuration types and the S3 `<ReplicationConfiguration>` XML parser.
//!
//! A bucket's replication configuration (ARCH §20.1) carries an IAM `<Role>` and a list of
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
    /// The first enabled rule whose prefix filter matches `key`, if any. Per S3, rules are
    /// evaluated in order and the first match wins for a given key.
    #[must_use]
    pub fn matching_rule(&self, key: &str) -> Option<&ReplicationRule> {
        self.rules
            .iter()
            .find(|r| r.enabled && r.filter.matches(key))
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
}

/// The selector that scopes a rule to a subset of a bucket's keys. An empty prefix matches
/// every key. Cairn supports the prefix filter (S3's `<Prefix>` and `<Filter><Prefix>` forms);
/// tag filters are accepted but not yet honoured.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Filter {
    /// Restrict to keys beginning with this prefix. `None`/empty matches every key.
    pub prefix: Option<String>,
}

impl Filter {
    /// Whether a key matches this filter.
    #[must_use]
    pub fn matches(&self, key: &str) -> bool {
        match &self.prefix {
            Some(p) => key.starts_with(p.as_str()),
            None => true,
        }
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

    // A stack of currently-open element local names, so a parser can disambiguate where a text
    // leaf belongs (e.g. `Bucket` under `Destination` vs a stray element).
    let mut stack: Vec<Vec<u8>> = Vec::new();

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                if name.as_slice() == b"Rule" {
                    in_rule = true;
                    rule = ReplicationRule::default();
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
                    b"Status" => {
                        rule.enabled = match text.trim() {
                            "Enabled" => true,
                            "Disabled" => false,
                            _ => return Err(malformed()),
                        };
                    }
                    b"Prefix" => {
                        // `<Prefix>` may appear directly under `<Rule>` or nested in `<Filter>`
                        // / `<And>`; treat all positions as the rule prefix.
                        rule.filter.prefix = Some(text.into_owned());
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
                if name.as_slice() == b"Rule" && in_rule {
                    config.rules.push(std::mem::take(&mut rule));
                    in_rule = false;
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
            }],
        };
        assert!(matches!(
            no_dest.validate(VersioningState::Enabled),
            Err(Error::InvalidRequest(_))
        ));
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
