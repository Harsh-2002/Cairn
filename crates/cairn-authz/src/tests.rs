//! Table-driven matrix and property tests for the evaluation order (ARCH §15.3).

use super::*;
use cairn_types::authz::{ActionPattern, Condition, ConditionOperator, PrincipalSpec};
use cairn_types::{
    Acl, Action, AuthzInput, BucketName, Decision, DenyReason, Effect, Grant, Grantee, ObjectKey,
    OwnershipMode, Permission, Policy, PublicAccessBlock, RequestContext, RequesterClass, Resource,
    Statement, Timestamp, UserId,
};
use std::net::{IpAddr, Ipv4Addr};

fn uid(s: &str) -> UserId {
    UserId(s.to_owned())
}

fn object_resource() -> Resource {
    Resource::Object {
        bucket: BucketName::parse("my-bucket").unwrap(),
        key: ObjectKey::parse("photos/a.jpg").unwrap(),
    }
}

fn ctx() -> RequestContext {
    RequestContext {
        source: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
        secure_transport: true,
        referer: None,
        user_agent: Some("aws-cli/2.0".to_owned()),
        now: Timestamp::from_secs(1_700_000_000),
        prefix: None,
        delimiter: None,
        max_keys: None,
        canned_acl: None,
        content_sha256: None,
        version_id: None,
        existing_tags: vec![],
        request_tags: vec![],
    }
}

/// A baseline input: anonymous member, GetObject on an object, no policy, no ACLs, BPA off,
/// ownership mode keeps ACLs in force.
fn base_input() -> AuthzInput {
    AuthzInput {
        requester: RequesterClass::Anonymous,
        action: Action::GetObject,
        resource: object_resource(),
        bucket_owner: uid("owner"),
        account_bpa: PublicAccessBlock::default(),
        bucket_bpa: PublicAccessBlock::default(),
        policy: None,
        bucket_acl: None,
        object_acl: None,
        ownership_mode: OwnershipMode::ObjectWriter,
        context: ctx(),
    }
}

fn public_read_object_acl() -> Acl {
    Acl {
        owner: uid("owner"),
        grants: vec![
            Grant {
                grantee: Grantee::User(uid("owner")),
                permission: Permission::FullControl,
            },
            Grant {
                grantee: Grantee::AllUsers,
                permission: Permission::Read,
            },
        ],
    }
}

fn allow_get_statement(principals: PrincipalSpec, conditions: Vec<Condition>) -> Statement {
    Statement {
        sid: None,
        effect: Effect::Allow,
        principals,
        actions: vec![ActionPattern::Exact("s3:GetObject".to_owned())],
        resources: vec!["arn:aws:s3:::my-bucket/*".to_owned()],
        conditions,
    }
}

fn deny_get_statement(principals: PrincipalSpec) -> Statement {
    Statement {
        sid: None,
        effect: Effect::Deny,
        principals,
        actions: vec![ActionPattern::All],
        resources: vec!["arn:aws:s3:::my-bucket/*".to_owned()],
        conditions: vec![],
    }
}

fn policy(statements: Vec<Statement>) -> Policy {
    Policy {
        version: "2012-10-17".to_owned(),
        id: None,
        statements,
    }
}

fn string_equals(key: &str, val: &str) -> Condition {
    Condition {
        operator: ConditionOperator::StringEquals,
        key: key.to_owned(),
        values: vec![val.to_owned()],
        if_exists: false,
    }
}

// --- The gate test matrix --------------------------------------------------------------

#[test]
fn admin_allowed() {
    let mut input = base_input();
    input.requester = RequesterClass::OwnerOrAdmin;
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn owner_allowed_even_with_no_policy_or_acl() {
    let mut input = base_input();
    input.requester = RequesterClass::OwnerOrAdmin;
    input.policy = None;
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn owner_can_be_locked_out_by_explicit_deny() {
    let mut input = base_input();
    input.requester = RequesterClass::OwnerOrAdmin;
    input.policy = Some(policy(vec![deny_get_statement(PrincipalSpec::Any)]));
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::ExplicitPolicyDeny)
    );
}

#[test]
fn anonymous_public_read_acl_allows_get_object() {
    let mut input = base_input();
    input.requester = RequesterClass::Anonymous;
    input.object_acl = Some(public_read_object_acl());
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn public_read_acl_denied_when_ignore_public_acls() {
    let mut input = base_input();
    input.requester = RequesterClass::Anonymous;
    input.object_acl = Some(public_read_object_acl());
    input.bucket_bpa.ignore_public_acls = true;
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::BlockPublicAccess)
    );
}

#[test]
fn explicit_policy_deny_overrides_allow() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    input.policy = Some(policy(vec![
        allow_get_statement(PrincipalSpec::Users(vec![uid("alice")]), vec![]),
        deny_get_statement(PrincipalSpec::Users(vec![uid("alice")])),
    ]));
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::ExplicitPolicyDeny)
    );
}

#[test]
fn member_with_no_grant_is_default_denied() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("bob"));
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn bucket_owner_enforced_ignores_acl_grants() {
    // An object ACL granting AllUsers Read does NOT apply under BucketOwnerEnforced.
    let mut input = base_input();
    input.requester = RequesterClass::Anonymous;
    input.object_acl = Some(public_read_object_acl());
    input.ownership_mode = OwnershipMode::BucketOwnerEnforced;
    // Nothing else grants it, and the public ACL is ignored, so default deny (not BPA).
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn bucket_owner_enforced_user_grant_also_ignored() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    input.object_acl = Some(Acl {
        owner: uid("owner"),
        grants: vec![Grant {
            grantee: Grantee::User(uid("alice")),
            permission: Permission::Read,
        }],
    });
    input.ownership_mode = OwnershipMode::BucketOwnerEnforced;
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));

    // But with ACLs in force the same grant allows.
    input.ownership_mode = OwnershipMode::ObjectWriter;
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn matching_string_equals_condition_allows() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    input.context.prefix = Some("photos/".to_owned());
    input.action = Action::GetObject;
    input.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Users(vec![uid("alice")]),
        vec![string_equals("aws:UserAgent", "aws-cli/2.0")],
    )]));
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn non_matching_string_equals_condition_denies() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    input.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Users(vec![uid("alice")]),
        vec![string_equals("aws:UserAgent", "curl/8.0")],
    )]));
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn unknown_condition_key_denies() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    input.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Users(vec![uid("alice")]),
        vec![string_equals("aws:NoSuchKey", "whatever")],
    )]));
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn public_policy_grant_denied_when_block_public_policy() {
    let mut input = base_input();
    input.requester = RequesterClass::Anonymous;
    input.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Any,
        vec![],
    )]));
    // Without BPA, anonymous is allowed by the public policy.
    assert_eq!(evaluate(&input), Decision::Allow);
    // With block_public_policy, the public policy grant is neutralised.
    input.bucket_bpa.block_public_policy = true;
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::BlockPublicAccess)
    );
}

#[test]
fn restrict_public_buckets_also_neutralises_public_policy() {
    let mut input = base_input();
    input.requester = RequesterClass::Anonymous;
    input.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Any,
        vec![],
    )]));
    input.account_bpa.restrict_public_buckets = true;
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::BlockPublicAccess)
    );
}

#[test]
fn bpa_does_not_deny_when_a_non_public_grant_exists() {
    // An authenticated member granted by a user-principal policy is not "public", so BPA
    // toggles do not deny them even when public grants are also present.
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    input.policy = Some(policy(vec![
        allow_get_statement(PrincipalSpec::Any, vec![]),
        allow_get_statement(PrincipalSpec::Users(vec![uid("alice")]), vec![]),
    ]));
    input.bucket_bpa.block_public_policy = true;
    input.bucket_bpa.restrict_public_buckets = true;
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn account_and_bucket_bpa_union() {
    // Account-level ignore_public_acls suppresses even when bucket-level is off.
    let mut input = base_input();
    input.requester = RequesterClass::Anonymous;
    input.object_acl = Some(public_read_object_acl());
    input.account_bpa.ignore_public_acls = true;
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::BlockPublicAccess)
    );
}

#[test]
fn bucket_listing_via_bucket_read_acl() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    input.action = Action::ListBucket;
    input.resource = Resource::Bucket(BucketName::parse("my-bucket").unwrap());
    input.bucket_acl = Some(Acl {
        owner: uid("owner"),
        grants: vec![Grant {
            grantee: Grantee::AuthenticatedUsers,
            permission: Permission::Read,
        }],
    });
    assert_eq!(evaluate(&input), Decision::Allow);

    // Anonymous (not authenticated) is not in the AuthenticatedUsers group.
    input.requester = RequesterClass::Anonymous;
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn parse_policy_round_trip_and_evaluate() {
    let json = r#"{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::my-bucket/*"
        }]
    }"#;
    let parsed = parse_policy(json).unwrap();
    let mut input = base_input();
    input.requester = RequesterClass::Anonymous;
    input.policy = Some(parsed);
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn parse_policy_rejects_malformed() {
    assert!(matches!(
        parse_policy("{ this is not json"),
        Err(cairn_types::Error::MalformedPolicy)
    ));
}

#[test]
fn policy_grants_public_detects_wildcard_allow() {
    let p = policy(vec![allow_get_statement(PrincipalSpec::Any, vec![])]);
    assert!(policy_grants_public(&p));
    let p = policy(vec![allow_get_statement(
        PrincipalSpec::Users(vec![uid("alice")]),
        vec![],
    )]);
    assert!(!policy_grants_public(&p));
}

// --- Property tests --------------------------------------------------------------------

mod props {
    use super::*;
    use proptest::prelude::*;

    fn arb_action() -> impl Strategy<Value = Action> {
        prop_oneof![
            Just(Action::GetObject),
            Just(Action::PutObject),
            Just(Action::DeleteObject),
            Just(Action::ListBucket),
            Just(Action::GetObjectAcl),
            Just(Action::PutObjectAcl),
        ]
    }

    fn arb_requester() -> impl Strategy<Value = RequesterClass> {
        prop_oneof![
            Just(RequesterClass::OwnerOrAdmin),
            Just(RequesterClass::Anonymous),
            "[a-z]{1,6}".prop_map(|s| RequesterClass::AuthenticatedMember(uid(&s))),
        ]
    }

    fn arb_bpa() -> impl Strategy<Value = PublicAccessBlock> {
        (any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>()).prop_map(|(a, b, c, d)| {
            PublicAccessBlock {
                block_public_acls: a,
                ignore_public_acls: b,
                block_public_policy: c,
                restrict_public_buckets: d,
            }
        })
    }

    fn arb_principal() -> impl Strategy<Value = PrincipalSpec> {
        prop_oneof![
            Just(PrincipalSpec::Any),
            "[a-z]{1,6}".prop_map(|s| PrincipalSpec::Users(vec![uid(&s)])),
        ]
    }

    fn arb_effect() -> impl Strategy<Value = Effect> {
        prop_oneof![Just(Effect::Allow), Just(Effect::Deny)]
    }

    fn arb_action_pattern() -> impl Strategy<Value = ActionPattern> {
        prop_oneof![
            Just(ActionPattern::All),
            Just(ActionPattern::Prefix("s3:Get".to_owned())),
            Just(ActionPattern::Exact("s3:GetObject".to_owned())),
            Just(ActionPattern::Exact("s3:PutObject".to_owned())),
        ]
    }

    fn arb_statement() -> impl Strategy<Value = Statement> {
        (
            arb_effect(),
            arb_principal(),
            prop::collection::vec(arb_action_pattern(), 1..3),
        )
            .prop_map(|(effect, principals, actions)| Statement {
                sid: None,
                effect,
                principals,
                actions,
                resources: vec![
                    "arn:aws:s3:::my-bucket".to_owned(),
                    "arn:aws:s3:::my-bucket/*".to_owned(),
                ],
                conditions: vec![],
            })
    }

    fn arb_policy() -> impl Strategy<Value = Option<Policy>> {
        prop::option::of(
            prop::collection::vec(arb_statement(), 0..5).prop_map(|statements| Policy {
                version: "2012-10-17".to_owned(),
                id: None,
                statements,
            }),
        )
    }

    fn arb_ownership() -> impl Strategy<Value = OwnershipMode> {
        prop_oneof![
            Just(OwnershipMode::BucketOwnerEnforced),
            Just(OwnershipMode::BucketOwnerPreferred),
            Just(OwnershipMode::ObjectWriter),
        ]
    }

    fn arb_input() -> impl Strategy<Value = AuthzInput> {
        (
            arb_requester(),
            arb_action(),
            arb_bpa(),
            arb_bpa(),
            arb_policy(),
            arb_ownership(),
        )
            .prop_map(
                |(requester, action, account_bpa, bucket_bpa, policy, ownership_mode)| AuthzInput {
                    requester,
                    action,
                    resource: object_resource(),
                    bucket_owner: uid("owner"),
                    account_bpa,
                    bucket_bpa,
                    policy,
                    bucket_acl: None,
                    object_acl: Some(public_read_object_acl()),
                    ownership_mode,
                    context: ctx(),
                },
            )
    }

    proptest! {
        // Property 1: an explicit Deny that matches ALWAYS yields Deny, regardless of any
        // Allow statements present (for a non-owner; owner has its own deny path too).
        #[test]
        fn matching_explicit_deny_always_denies(mut input in arb_input()) {
            // Force a matching explicit Deny: a wildcard-principal deny on all actions/resources.
            let mut statements = match input.policy.take() {
                Some(p) => p.statements,
                None => vec![],
            };
            statements.push(Statement {
                sid: None,
                effect: Effect::Deny,
                principals: PrincipalSpec::Any,
                actions: vec![ActionPattern::All],
                resources: vec![
                    "arn:aws:s3:::my-bucket".to_owned(),
                    "arn:aws:s3:::my-bucket/*".to_owned(),
                ],
                conditions: vec![],
            });
            input.policy = Some(Policy {
                version: "2012-10-17".to_owned(),
                id: None,
                statements,
            });

            // A matching explicit Deny must NEVER be overridden into an Allow, regardless of
            // any Allow statements present. (BPA may deny earlier in the fixed order, which is
            // still a Deny; the cornerstone invariant is "explicit deny is never an Allow".)
            let decision = evaluate(&input);
            prop_assert!(matches!(decision, Decision::Deny(_)), "got {decision:?}");

            // When BPA is not in play (no public-only grant being suppressed), the explicit
            // deny is specifically the reason. Force that by clearing public ACL grants and
            // BPA toggles so step (b) cannot fire.
            let mut clean = input.clone();
            clean.object_acl = None;
            clean.bucket_acl = None;
            clean.account_bpa = PublicAccessBlock::default();
            clean.bucket_bpa = PublicAccessBlock::default();
            let clean_decision = evaluate(&clean);
            // Owner/admin and non-owner alike: a matching all-actions wildcard Deny applies.
            prop_assert_eq!(clean_decision, Decision::Deny(DenyReason::ExplicitPolicyDeny));
        }

        // Property 2 (monotonicity): tightening BPA (turning more toggles on) never turns a
        // Deny into an Allow. We compare a baseline against a strictly-tighter BPA.
        #[test]
        fn tightening_bpa_never_allows_more(
            input in arb_input(),
            extra_account in arb_bpa(),
            extra_bucket in arb_bpa(),
        ) {
            let base = evaluate(&input);

            // Build a tighter input: OR each toggle with the extra (can only turn bits on).
            let mut tighter = input.clone();
            tighter.account_bpa = or_bpa(input.account_bpa, extra_account);
            tighter.bucket_bpa = or_bpa(input.bucket_bpa, extra_bucket);
            let after = evaluate(&tighter);

            // If the baseline denied, the tighter one must also deny (no Deny -> Allow).
            if matches!(base, Decision::Deny(_)) {
                prop_assert!(matches!(after, Decision::Deny(_)));
            }
        }
    }

    fn or_bpa(a: PublicAccessBlock, b: PublicAccessBlock) -> PublicAccessBlock {
        PublicAccessBlock {
            block_public_acls: a.block_public_acls || b.block_public_acls,
            ignore_public_acls: a.ignore_public_acls || b.ignore_public_acls,
            block_public_policy: a.block_public_policy || b.block_public_policy,
            restrict_public_buckets: a.restrict_public_buckets || b.restrict_public_buckets,
        }
    }
}
