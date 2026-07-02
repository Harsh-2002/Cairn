//! Table-driven matrix and property tests for the evaluation order (ARCH 15.3).

use super::*;
use cairn_types::authz::{
    ActionMatch, ActionPattern, Condition, ConditionOperator, PrincipalSpec, ResourceMatch,
};
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
        is_session: false,
        action: Action::GetObject,
        resource: object_resource(),
        bucket_owner: uid("owner"),
        account_bpa: PublicAccessBlock::default(),
        bucket_bpa: PublicAccessBlock::default(),
        policy: None,
        user_policy: None,
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
        actions: ActionMatch::In(vec![ActionPattern::Exact("s3:GetObject".to_owned())]),
        resources: ResourceMatch::In(vec!["arn:aws:s3:::my-bucket/*".to_owned()]),
        conditions,
    }
}

fn deny_get_statement(principals: PrincipalSpec) -> Statement {
    Statement {
        sid: None,
        effect: Effect::Deny,
        principals,
        actions: ActionMatch::In(vec![ActionPattern::All]),
        resources: ResourceMatch::In(vec!["arn:aws:s3:::my-bucket/*".to_owned()]),
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

/// An identity (per-user) policy statement: Principal-less (the engine ignores its principal), with
/// `effect` over a single exact `action` on `my-bucket/*`.
fn user_stmt(effect: Effect, action: &str) -> Statement {
    Statement {
        sid: None,
        effect,
        principals: PrincipalSpec::Any,
        actions: ActionMatch::In(vec![ActionPattern::Exact(action.to_owned())]),
        resources: ResourceMatch::In(vec!["arn:aws:s3:::my-bucket/*".to_owned()]),
        conditions: vec![],
    }
}

// --- STS session scoping (ARCH 14 / M2): a session is governed SOLELY by its own scoped policy ----

/// A request from a temporary session credential whose parent is `parent`.
fn session_input() -> AuthzInput {
    let mut i = base_input();
    i.requester = RequesterClass::AuthenticatedMember(uid("parent"));
    i.is_session = true;
    i
}

#[test]
fn session_not_widened_by_parent_named_bucket_policy() {
    let mut i = session_input();
    // A bucket policy Allowing the parent user widens a normal member (see the control below), but
    // must NOT widen a session — the session carries no such grant of its own.
    i.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Users(vec![uid("parent")]),
        vec![],
    )]));
    assert_eq!(evaluate(&i), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn session_not_widened_by_parent_user_acl() {
    let mut i = session_input(); // ownership_mode ObjectWriter -> ACLs in force
    i.object_acl = Some(Acl {
        owner: uid("owner"),
        grants: vec![Grant {
            grantee: Grantee::User(uid("parent")),
            permission: Permission::Read,
        }],
    });
    assert_eq!(evaluate(&i), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn session_governed_by_its_own_policy_and_explicit_deny_still_binds() {
    // (a) The session's own scoped policy grants -> Allow (guards against over-restricting).
    let mut i = session_input();
    i.user_policy = Some(policy(vec![user_stmt(Effect::Allow, "s3:GetObject")]));
    assert_eq!(evaluate(&i), Decision::Allow);
    // (b) An explicit bucket-policy Deny naming the parent still binds the session even though its
    //     own policy grants — fail-closed is intact for sessions.
    i.policy = Some(policy(vec![deny_get_statement(PrincipalSpec::Users(
        vec![uid("parent")],
    ))]));
    assert_eq!(evaluate(&i), Decision::Deny(DenyReason::ExplicitPolicyDeny));
}

#[test]
fn non_session_member_is_still_widened_by_parent_named_bucket_policy() {
    // Control: a real (non-session) member IS allowed by the same parent-named bucket policy, so the
    // gating above is specific to sessions, not a blanket restriction.
    let mut i = base_input();
    i.requester = RequesterClass::AuthenticatedMember(uid("parent"));
    i.is_session = false;
    i.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Users(vec![uid("parent")]),
        vec![],
    )]));
    assert_eq!(evaluate(&i), Decision::Allow);
}

// --- Identity (per-user) policy: ARCH 15 / user-centric authz --------------------------

#[test]
fn user_policy_grants_member_scoped_get_not_put() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    input.user_policy = Some(policy(vec![user_stmt(Effect::Allow, "s3:GetObject")]));
    // The identity policy grants GetObject...
    input.action = Action::GetObject;
    assert_eq!(evaluate(&input), Decision::Allow);
    // ...but nothing grants PutObject → default deny.
    input.action = Action::PutObject;
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn user_policy_deny_overrides_bucket_allow() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    // The bucket (resource) policy allows GetObject for alice...
    input.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Users(vec![uid("alice")]),
        vec![],
    )]));
    // ...but the user's identity policy explicitly Denies it — explicit Deny wins.
    input.user_policy = Some(policy(vec![user_stmt(Effect::Deny, "s3:GetObject")]));
    input.action = Action::GetObject;
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::ExplicitPolicyDeny)
    );
}

#[test]
fn admin_full_access_without_user_policy() {
    let mut input = base_input();
    input.requester = RequesterClass::OwnerOrAdmin;
    input.user_policy = None;
    input.action = Action::PutObject;
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn user_policy_deny_binds_owner() {
    // An identity Deny binds even an owner/admin acting as themselves (AWS semantics).
    let mut input = base_input();
    input.requester = RequesterClass::OwnerOrAdmin;
    input.user_policy = Some(policy(vec![user_stmt(Effect::Deny, "s3:GetObject")]));
    input.action = Action::GetObject;
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::ExplicitPolicyDeny)
    );
}

#[test]
fn union_of_bucket_and_identity_grants() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    // Bucket (resource) policy grants GetObject to alice; identity policy grants PutObject.
    input.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Users(vec![uid("alice")]),
        vec![],
    )]));
    input.user_policy = Some(policy(vec![user_stmt(Effect::Allow, "s3:PutObject")]));
    // The union grants both; an action neither grants is denied.
    input.action = Action::GetObject;
    assert_eq!(evaluate(&input), Decision::Allow);
    input.action = Action::PutObject;
    assert_eq!(evaluate(&input), Decision::Allow);
    input.action = Action::DeleteObject;
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn user_policy_grant_survives_block_public_access() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    // A public bucket-policy grant that BPA suppresses.
    input.policy = Some(policy(vec![allow_get_statement(
        PrincipalSpec::Any,
        vec![],
    )]));
    input.account_bpa = PublicAccessBlock {
        block_public_policy: true,
        restrict_public_buckets: true,
        ..PublicAccessBlock::default()
    };
    input.action = Action::GetObject;
    // Without an identity policy, the sole (public) grant is BPA-suppressed → deny.
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::BlockPublicAccess)
    );
    // An identity grant is never public, so it survives BPA → allow.
    input.user_policy = Some(policy(vec![user_stmt(Effect::Allow, "s3:GetObject")]));
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn parse_user_policy_allows_missing_principal() {
    // An identity policy may omit Principal (the principal is the attached user).
    let doc = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#;
    let p = crate::parse_user_policy(doc).expect("identity policy parses without Principal");
    assert_eq!(p.statements.len(), 1);
    assert_eq!(p.statements[0].effect, Effect::Allow);
    // The same document via parse_policy (which requires Principal) is rejected.
    assert!(crate::parse_policy(doc).is_err());
    // Malformed JSON is still rejected.
    assert!(crate::parse_user_policy("{ not json").is_err());
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
fn object_with_no_acl_does_not_inherit_a_public_bucket_acl() {
    // An object with no explicit ACL must be private to its owner — it must NOT pick up a
    // `public-read` BUCKET ACL grant, which would expose object contents to anyone (audit #2).
    let mut input = base_input();
    input.requester = RequesterClass::Anonymous;
    input.object_acl = None;
    input.bucket_acl = Some(public_read_object_acl()); // a public-read grant on the BUCKET
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::DefaultDeny),
        "anonymous GetObject must be denied when only the bucket ACL is public"
    );
    // Sanity: a public-read OBJECT ACL still grants (unchanged behavior).
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

// --- Negated policy forms: NotAction / NotResource / NotPrincipal (ARCH 15.5) ----------

#[test]
fn not_action_allow_grants_all_actions_except_listed() {
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("bob"));
    // Allow every action EXCEPT GetObject (NotAction) on the bucket's objects.
    input.policy = Some(policy(vec![Statement {
        sid: None,
        effect: Effect::Allow,
        principals: PrincipalSpec::Users(vec![uid("bob")]),
        actions: ActionMatch::NotIn(vec![ActionPattern::Exact("s3:GetObject".to_owned())]),
        resources: ResourceMatch::In(vec!["arn:aws:s3:::my-bucket/*".to_owned()]),
        conditions: vec![],
    }]));
    // PutObject is not excluded → the NotAction Allow grants it.
    input.action = Action::PutObject;
    assert_eq!(evaluate(&input), Decision::Allow);
    // GetObject is excluded → nothing grants it → default deny.
    input.action = Action::GetObject;
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn deny_not_resource_locks_out_everything_except_listed() {
    // The classic `Deny` + `NotResource` lockout: deny on every resource *except* a carve-out.
    let mut input = base_input();
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    input.policy = Some(policy(vec![
        // A broad grant so there is something to deny over.
        Statement {
            sid: None,
            effect: Effect::Allow,
            principals: PrincipalSpec::Users(vec![uid("alice")]),
            actions: ActionMatch::In(vec![ActionPattern::All]),
            resources: ResourceMatch::In(vec![
                "arn:aws:s3:::my-bucket".to_owned(),
                "arn:aws:s3:::my-bucket/*".to_owned(),
            ]),
            conditions: vec![],
        },
        // Deny everything whose resource is NOT under public/.
        Statement {
            sid: None,
            effect: Effect::Deny,
            principals: PrincipalSpec::Users(vec![uid("alice")]),
            actions: ActionMatch::In(vec![ActionPattern::All]),
            resources: ResourceMatch::NotIn(vec!["arn:aws:s3:::my-bucket/public/*".to_owned()]),
            conditions: vec![],
        },
    ]));
    // photos/a.jpg is not under public/ → the NotResource deny matches → locked out.
    input.resource = object_resource();
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::ExplicitPolicyDeny)
    );
    // A resource under public/ is exempt from the deny → the broad allow grants it.
    input.resource = Resource::Object {
        bucket: BucketName::parse("my-bucket").unwrap(),
        key: ObjectKey::parse("public/welcome.txt").unwrap(),
    };
    assert_eq!(evaluate(&input), Decision::Allow);
}

#[test]
fn not_principal_allow_grants_everyone_except_listed() {
    let mut input = base_input();
    input.action = Action::GetObject;
    input.policy = Some(policy(vec![Statement {
        sid: None,
        effect: Effect::Allow,
        principals: PrincipalSpec::NotUsers(vec![uid("alice")]),
        actions: ActionMatch::In(vec![ActionPattern::Exact("s3:GetObject".to_owned())]),
        resources: ResourceMatch::In(vec!["arn:aws:s3:::my-bucket/*".to_owned()]),
        conditions: vec![],
    }]));
    // bob is not excluded → the NotPrincipal Allow covers him.
    input.requester = RequesterClass::AuthenticatedMember(uid("bob"));
    assert_eq!(evaluate(&input), Decision::Allow);
    // alice is excluded → nothing grants her → default deny.
    input.requester = RequesterClass::AuthenticatedMember(uid("alice"));
    assert_eq!(evaluate(&input), Decision::Deny(DenyReason::DefaultDeny));
}

#[test]
fn not_principal_allow_is_public_and_suppressed_by_bpa() {
    let mut input = base_input();
    input.requester = RequesterClass::Anonymous;
    input.action = Action::GetObject;
    let pol = policy(vec![Statement {
        sid: None,
        effect: Effect::Allow,
        principals: PrincipalSpec::NotUsers(vec![uid("alice")]),
        actions: ActionMatch::In(vec![ActionPattern::Exact("s3:GetObject".to_owned())]),
        resources: ResourceMatch::In(vec!["arn:aws:s3:::my-bucket/*".to_owned()]),
        conditions: vec![],
    }]);
    // The management surface must flag a NotPrincipal Allow as a public grant.
    assert!(crate::policy_grants_public(&pol));
    input.policy = Some(pol);
    // Anonymous is "not any named user", so the NotPrincipal Allow covers it when BPA is off.
    assert_eq!(evaluate(&input), Decision::Allow);
    // Blocking public policy neutralises it exactly like a `"*"` grant.
    input.account_bpa = PublicAccessBlock {
        block_public_policy: true,
        restrict_public_buckets: true,
        ..PublicAccessBlock::default()
    };
    assert_eq!(
        evaluate(&input),
        Decision::Deny(DenyReason::BlockPublicAccess)
    );
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
            "[a-z]{1,6}".prop_map(|s| PrincipalSpec::NotUsers(vec![uid(&s)])),
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
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(
                |(effect, principals, actions, negate_action, negate_resource)| {
                    let actions = if negate_action {
                        ActionMatch::NotIn(actions)
                    } else {
                        ActionMatch::In(actions)
                    };
                    let resources = vec![
                        "arn:aws:s3:::my-bucket".to_owned(),
                        "arn:aws:s3:::my-bucket/*".to_owned(),
                    ];
                    let resources = if negate_resource {
                        ResourceMatch::NotIn(resources)
                    } else {
                        ResourceMatch::In(resources)
                    };
                    Statement {
                        sid: None,
                        effect,
                        principals,
                        actions,
                        resources,
                        conditions: vec![],
                    }
                },
            )
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
                    is_session: false,
                    action,
                    resource: object_resource(),
                    bucket_owner: uid("owner"),
                    account_bpa,
                    bucket_bpa,
                    policy,
                    user_policy: None,
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
                actions: ActionMatch::In(vec![ActionPattern::All]),
                resources: ResourceMatch::In(vec![
                    "arn:aws:s3:::my-bucket".to_owned(),
                    "arn:aws:s3:::my-bucket/*".to_owned(),
                ]),
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
