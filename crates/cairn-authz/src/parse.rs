//! Parsing of AWS-style bucket-policy JSON into [`cairn_types::Policy`] (ARCH §15.5).
//!
//! Structural problems yield [`cairn_types::Error::MalformedPolicy`].

use cairn_types::authz::{ActionPattern, Condition, ConditionOperator, NumericOp, PrincipalSpec};
use cairn_types::{Effect, Error, Policy, Statement, UserId};
use serde_json::Value;

/// Parse an AWS-style bucket-policy JSON document into a [`Policy`].
///
/// Supports: `Version`, optional `Id`, a `Statement` that is one object or an array; each
/// statement's optional `Sid`, `Effect` (`Allow`/`Deny`), `Principal` (`"*"` or
/// `{"AWS": id | [ids]}`), `Action` (string or array; `s3:*`, `s3:Get*`, exact), `Resource`
/// (string or array of ARNs), and an optional `Condition` block mapping operator names to
/// `{ key: value | [values] }`.
///
/// # Errors
/// Returns [`Error::MalformedPolicy`] for any structural problem (bad JSON, missing required
/// fields, wrong shapes, unknown effect, or an unknown condition operator).
pub fn parse_policy(json: &str) -> Result<Policy, Error> {
    parse_policy_inner(json, true)
}

/// Parse a Principal-less **identity** (per-user) policy (ARCH §15 / user-centric authz).
///
/// Identical to [`parse_policy`], except a statement may omit `Principal`: the principal is
/// implicitly the user the policy is attached to. An omitted `Principal` parses to
/// [`PrincipalSpec::Any`] as a parse-time default; the engine evaluates user-policy statements on a
/// dedicated path that never consults the principal, so the stored value is irrelevant.
///
/// # Errors
/// Returns [`Error::MalformedPolicy`] for any structural problem (as [`parse_policy`]).
pub fn parse_user_policy(json: &str) -> Result<Policy, Error> {
    parse_policy_inner(json, false)
}

fn parse_policy_inner(json: &str, require_principal: bool) -> Result<Policy, Error> {
    let root: Value = serde_json::from_str(json).map_err(|_| Error::MalformedPolicy)?;
    let obj = root.as_object().ok_or(Error::MalformedPolicy)?;

    let version = obj
        .get("Version")
        .and_then(Value::as_str)
        .ok_or(Error::MalformedPolicy)?
        .to_owned();

    let id = match obj.get("Id") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(Error::MalformedPolicy),
    };

    let raw_statements = obj.get("Statement").ok_or(Error::MalformedPolicy)?;
    let statements = match raw_statements {
        Value::Array(arr) => arr
            .iter()
            .map(|s| parse_statement(s, require_principal))
            .collect::<Result<_, _>>()?,
        Value::Object(_) => vec![parse_statement(raw_statements, require_principal)?],
        _ => return Err(Error::MalformedPolicy),
    };

    Ok(Policy {
        version,
        id,
        statements,
    })
}

fn parse_statement(v: &Value, require_principal: bool) -> Result<Statement, Error> {
    let obj = v.as_object().ok_or(Error::MalformedPolicy)?;

    let sid = match obj.get("Sid") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(Error::MalformedPolicy),
    };

    let effect = match obj.get("Effect").and_then(Value::as_str) {
        Some("Allow") => Effect::Allow,
        Some("Deny") => Effect::Deny,
        _ => return Err(Error::MalformedPolicy),
    };

    let principals = match obj.get("Principal") {
        Some(p) => parse_principal(p)?,
        None if require_principal => return Err(Error::MalformedPolicy),
        // Identity (per-user) policy: the principal is the attached user; default to `Any`.
        None => PrincipalSpec::Any,
    };
    let actions = parse_actions(obj.get("Action").ok_or(Error::MalformedPolicy)?)?;
    let resources = parse_resources(obj.get("Resource").ok_or(Error::MalformedPolicy)?)?;
    let conditions = match obj.get("Condition") {
        None | Some(Value::Null) => Vec::new(),
        Some(c) => parse_conditions(c)?,
    };

    Ok(Statement {
        sid,
        effect,
        principals,
        actions,
        resources,
        conditions,
    })
}

/// Parse `Principal`: `"*"` => [`PrincipalSpec::Any`]; `{"AWS": id | [ids]}` => the listed
/// users. A `{"AWS": "*"}` is also treated as `Any`.
fn parse_principal(v: &Value) -> Result<PrincipalSpec, Error> {
    match v {
        Value::String(s) if s == "*" => Ok(PrincipalSpec::Any),
        Value::Object(map) => {
            let aws = map.get("AWS").ok_or(Error::MalformedPolicy)?;
            match aws {
                Value::String(s) if s == "*" => Ok(PrincipalSpec::Any),
                Value::String(s) => Ok(PrincipalSpec::Users(vec![UserId(s.clone())])),
                Value::Array(arr) => {
                    let mut ids = Vec::with_capacity(arr.len());
                    for item in arr {
                        let s = item.as_str().ok_or(Error::MalformedPolicy)?;
                        if s == "*" {
                            return Ok(PrincipalSpec::Any);
                        }
                        ids.push(UserId(s.to_owned()));
                    }
                    Ok(PrincipalSpec::Users(ids))
                }
                _ => Err(Error::MalformedPolicy),
            }
        }
        _ => Err(Error::MalformedPolicy),
    }
}

/// Parse `Action` (string or array of strings) into [`ActionPattern`]s.
fn parse_actions(v: &Value) -> Result<Vec<ActionPattern>, Error> {
    let strings = as_string_or_array(v)?;
    if strings.is_empty() {
        return Err(Error::MalformedPolicy);
    }
    Ok(strings.iter().map(|s| parse_action_pattern(s)).collect())
}

/// `s3:*` => [`ActionPattern::All`]; `s3:Get*` => [`ActionPattern::Prefix("s3:Get")`];
/// exact => [`ActionPattern::Exact`]. A bare `*` also means all.
fn parse_action_pattern(s: &str) -> ActionPattern {
    if s == "*" || s == "s3:*" {
        ActionPattern::All
    } else if let Some(prefix) = s.strip_suffix('*') {
        ActionPattern::Prefix(prefix.to_owned())
    } else {
        ActionPattern::Exact(s.to_owned())
    }
}

/// Parse `Resource` (string or array of strings) into raw ARN-like resource patterns.
fn parse_resources(v: &Value) -> Result<Vec<String>, Error> {
    let strings = as_string_or_array(v)?;
    if strings.is_empty() {
        return Err(Error::MalformedPolicy);
    }
    Ok(strings)
}

/// Parse the `Condition` block: `{ OperatorName: { key: value | [values] } }`.
fn parse_conditions(v: &Value) -> Result<Vec<Condition>, Error> {
    let obj = v.as_object().ok_or(Error::MalformedPolicy)?;
    let mut out = Vec::new();
    for (op_name, keys) in obj {
        let (operator, if_exists) = parse_operator(op_name)?;
        let key_map = keys.as_object().ok_or(Error::MalformedPolicy)?;
        for (key, vals) in key_map {
            let values = as_string_or_array(vals)?;
            out.push(Condition {
                operator,
                key: key.clone(),
                values,
                if_exists,
            });
        }
    }
    Ok(out)
}

/// Map an AWS condition operator name (with an optional `IfExists` suffix) to a
/// [`ConditionOperator`] plus the `if_exists` flag.
fn parse_operator(name: &str) -> Result<(ConditionOperator, bool), Error> {
    let (base, if_exists) = match name.strip_suffix("IfExists") {
        Some(stripped) => (stripped, true),
        None => (name, false),
    };
    let op = match base {
        "StringEquals" => ConditionOperator::StringEquals,
        "StringNotEquals" => ConditionOperator::StringNotEquals,
        "StringLike" => ConditionOperator::StringLike,
        "Bool" => ConditionOperator::Bool,
        "IpAddress" => ConditionOperator::IpAddress,
        "NotIpAddress" => ConditionOperator::NotIpAddress,
        "NumericEquals" => ConditionOperator::Numeric(NumericOp::Equals),
        "NumericNotEquals" => ConditionOperator::Numeric(NumericOp::NotEquals),
        "NumericLessThan" => ConditionOperator::Numeric(NumericOp::LessThan),
        "NumericLessThanEquals" => ConditionOperator::Numeric(NumericOp::LessThanEquals),
        "NumericGreaterThan" => ConditionOperator::Numeric(NumericOp::GreaterThan),
        "NumericGreaterThanEquals" => ConditionOperator::Numeric(NumericOp::GreaterThanEquals),
        "DateEquals" => ConditionOperator::Date(NumericOp::Equals),
        "DateNotEquals" => ConditionOperator::Date(NumericOp::NotEquals),
        "DateLessThan" => ConditionOperator::Date(NumericOp::LessThan),
        "DateLessThanEquals" => ConditionOperator::Date(NumericOp::LessThanEquals),
        "DateGreaterThan" => ConditionOperator::Date(NumericOp::GreaterThan),
        "DateGreaterThanEquals" => ConditionOperator::Date(NumericOp::GreaterThanEquals),
        "Null" => ConditionOperator::Null,
        _ => return Err(Error::MalformedPolicy),
    };
    Ok((op, if_exists))
}

/// Coerce a JSON value that is either a string or an array of strings into `Vec<String>`.
fn as_string_or_array(v: &Value) -> Result<Vec<String>, Error> {
    match v {
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Array(arr) => arr
            .iter()
            .map(|item| match item {
                Value::String(s) => Ok(s.clone()),
                Value::Bool(b) => Ok(b.to_string()),
                Value::Number(n) => Ok(n.to_string()),
                _ => Err(Error::MalformedPolicy),
            })
            .collect(),
        Value::Bool(b) => Ok(vec![b.to_string()]),
        Value::Number(n) => Ok(vec![n.to_string()]),
        _ => Err(Error::MalformedPolicy),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPRESENTATIVE: &str = r#"{
        "Version": "2012-10-17",
        "Id": "ExamplePolicy",
        "Statement": [
            {
                "Sid": "PublicRead",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                "Resource": "arn:aws:s3:::my-bucket/*",
                "Condition": {
                    "IpAddress": { "aws:SourceIp": ["10.0.0.0/24", "192.168.0.0/16"] },
                    "Bool": { "aws:SecureTransport": "true" }
                }
            },
            {
                "Sid": "DenyOldAgents",
                "Effect": "Deny",
                "Principal": { "AWS": ["user-a", "user-b"] },
                "Action": "s3:*",
                "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"]
            }
        ]
    }"#;

    #[test]
    fn round_trips_representative_policy() {
        let policy = parse_policy(REPRESENTATIVE).expect("should parse");
        assert_eq!(policy.version, "2012-10-17");
        assert_eq!(policy.id.as_deref(), Some("ExamplePolicy"));
        assert_eq!(policy.statements.len(), 2);

        let s0 = &policy.statements[0];
        assert_eq!(s0.sid.as_deref(), Some("PublicRead"));
        assert_eq!(s0.effect, Effect::Allow);
        assert_eq!(s0.principals, PrincipalSpec::Any);
        assert_eq!(
            s0.actions,
            vec![
                ActionPattern::Exact("s3:GetObject".to_owned()),
                ActionPattern::Exact("s3:GetObjectVersion".to_owned())
            ]
        );
        assert_eq!(s0.resources, vec!["arn:aws:s3:::my-bucket/*".to_owned()]);
        assert_eq!(s0.conditions.len(), 2);
        assert!(
            s0.conditions
                .iter()
                .any(|c| c.operator == ConditionOperator::IpAddress
                    && c.key == "aws:SourceIp"
                    && c.values.len() == 2)
        );

        let s1 = &policy.statements[1];
        assert_eq!(s1.effect, Effect::Deny);
        assert_eq!(
            s1.principals,
            PrincipalSpec::Users(vec![
                UserId("user-a".to_owned()),
                UserId("user-b".to_owned())
            ])
        );
        assert_eq!(s1.actions, vec![ActionPattern::All]);
        assert_eq!(s1.resources.len(), 2);
    }

    #[test]
    fn single_statement_object() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::b/*"
            }
        }"#;
        let policy = parse_policy(json).unwrap();
        assert_eq!(policy.statements.len(), 1);
        assert!(policy.id.is_none());
    }

    #[test]
    fn prefix_action_and_principal_wildcard_in_array() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Principal": { "AWS": ["*"] },
                "Action": "s3:Get*",
                "Resource": "arn:aws:s3:::b"
            }
        }"#;
        let p = parse_policy(json).unwrap();
        assert_eq!(p.statements[0].principals, PrincipalSpec::Any);
        assert_eq!(
            p.statements[0].actions,
            vec![ActionPattern::Prefix("s3:Get".to_owned())]
        );
    }

    #[test]
    fn if_exists_operator() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::b/*",
                "Condition": { "StringLikeIfExists": { "aws:Referer": "https://*" } }
            }
        }"#;
        let p = parse_policy(json).unwrap();
        let c = &p.statements[0].conditions[0];
        assert_eq!(c.operator, ConditionOperator::StringLike);
        assert!(c.if_exists);
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(matches!(
            parse_policy("not json at all"),
            Err(Error::MalformedPolicy)
        ));
        assert!(matches!(parse_policy("{}"), Err(Error::MalformedPolicy)));
        assert!(matches!(parse_policy("[]"), Err(Error::MalformedPolicy)));
    }

    #[test]
    fn rejects_missing_required_fields() {
        // Missing Effect.
        let json = r#"{"Version":"2012-10-17","Statement":{"Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}}"#;
        assert!(matches!(parse_policy(json), Err(Error::MalformedPolicy)));
        // Missing Version.
        let json = r#"{"Statement":{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}}"#;
        assert!(matches!(parse_policy(json), Err(Error::MalformedPolicy)));
    }

    #[test]
    fn rejects_unknown_effect_and_operator() {
        let json = r#"{"Version":"2012-10-17","Statement":{"Effect":"Maybe","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}}"#;
        assert!(matches!(parse_policy(json), Err(Error::MalformedPolicy)));
        let json = r#"{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"Bogus":{"k":"v"}}}}"#;
        assert!(matches!(parse_policy(json), Err(Error::MalformedPolicy)));
    }
}
