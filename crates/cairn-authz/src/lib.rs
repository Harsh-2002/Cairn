//! `cairn-authz` — the pure authorization engine (ARCH §15).
//!
//! [`PolicyEngine`] implements [`cairn_types::AuthorizationEngine`] with the fixed evaluation
//! order of ARCH §15.3: owner/admin short-circuit (subject to explicit deny), the Block
//! Public Access gate, explicit policy deny, any-allow (policy or ACL), then default deny.
//! Everything here is a pure function of the [`AuthzInput`]; there is no I/O.
//!
//! The crate also exposes the policy parser ([`parse_policy`]) and the canned-ACL expander
//! ([`expand_canned_acl`]) that the protocol/control layers use when storing configuration.

#![forbid(unsafe_code)]

mod acl;
mod condition;
mod matching;
mod parse;

pub use acl::{expand_canned_acl, permission_satisfies};
pub use matching::{resource_arn, resource_matches, wildcard_match};
pub use parse::{parse_policy, parse_user_policy};

use cairn_types::authz::{ActionPattern, PrincipalSpec};
use cairn_types::{
    Acl, Action, AuthorizationEngine, AuthzInput, Decision, DenyReason, Effect, Grantee,
    OwnershipMode, Policy, PublicAccessBlock, RequesterClass, Resource, Statement, UserId,
};

use condition::conditions_match;

/// The production authorization engine. Stateless and cheap to construct; share one instance.
#[derive(Debug, Clone, Copy, Default)]
pub struct PolicyEngine;

impl AuthorizationEngine for PolicyEngine {
    fn evaluate(&self, input: &AuthzInput) -> Decision {
        evaluate(input)
    }
}

/// The free-function form of the evaluation, exposed so callers can evaluate without a trait
/// object. Identical to [`PolicyEngine::evaluate`].
#[must_use]
pub fn evaluate(input: &AuthzInput) -> Decision {
    // (a) Owner / admin: permitted unless an explicit Deny matches — in the bucket policy OR the
    //     requester's own identity policy (an identity Deny binds the principal, even an owner,
    //     matching AWS).
    if matches!(input.requester, RequesterClass::OwnerOrAdmin) {
        if explicit_deny_matches(input) || user_policy_deny_matches(input) {
            return Decision::Deny(DenyReason::ExplicitPolicyDeny);
        }
        return Decision::Allow;
    }

    // (b) Block Public Access gate: if the only thing that could grant this request is a
    //     public grant, and BPA suppresses public grants, deny here.
    if block_public_access_denies(input) {
        return Decision::Deny(DenyReason::BlockPublicAccess);
    }

    // (c) Explicit Deny anywhere (bucket policy or the requester's identity policy) denies
    //     unconditionally.
    if explicit_deny_matches(input) || user_policy_deny_matches(input) {
        return Decision::Deny(DenyReason::ExplicitPolicyDeny);
    }

    // (d) Any Allow: a matching bucket-policy Allow, a matching identity-policy Allow, or (ACLs in
    //     force) a matching ACL grant. This is the AWS union of resource- and identity-based grants.
    if policy_allow_matches_scoped(input, true, true)
        || user_policy_allow_matches(input)
        || acl_allows_scoped(input, true)
    {
        return Decision::Allow;
    }

    // (e) Default deny.
    Decision::Deny(DenyReason::DefaultDeny)
}

/// The Block Public Access gate (ARCH §15.3 step 2).
///
/// We compute whether the request would be granted at all, and whether it would still be
/// granted with public grants removed (as BPA neutralises them). If something grants it now
/// but nothing grants it once public grants are stripped, then the *only* thing granting it
/// was public, and BPA denies.
///
/// Which public grants are stripped depends on the toggles, evaluated as the union of account
/// and bucket settings (the stricter wins, matching S3 where the effective setting is the OR):
/// `ignore_public_acls` neutralises public ACL grants; `block_public_policy` /
/// `restrict_public_buckets` neutralise public policy grants for anonymous/cross-account
/// requesters.
fn block_public_access_denies(input: &AuthzInput) -> bool {
    let effective = effective_bpa(input.account_bpa, input.bucket_bpa);

    let suppress_public_acls = effective.ignore_public_acls;
    // Either toggle restricting public buckets/policies neutralises a public *policy* grant.
    let suppress_public_policy = effective.block_public_policy || effective.restrict_public_buckets;

    // If nothing public is being suppressed, BPA cannot be the deciding factor.
    if !suppress_public_acls && !suppress_public_policy {
        return false;
    }

    // Does *anything* grant the request today (including public grants)?
    let granted_with_public =
        policy_allow_matches_scoped(input, true, true) || acl_allows_scoped(input, true);

    if !granted_with_public {
        // Nothing grants it anyway; default deny will handle it (not a BPA denial).
        return false;
    }

    // Does anything still grant it once the suppressed public grants are removed? Identity (per-user)
    // policy grants are never public, so they always count here — a user-policy grant survives BPA.
    let granted_without_public = policy_allow_matches_scoped(
        input,
        /* allow_public_principal = */ !suppress_public_policy,
        /* allow_user_principal = */ true,
    ) || user_policy_allow_matches(input)
        || acl_allows_scoped(
            input,
            /* allow_public_grantees = */ !suppress_public_acls,
        );

    // Public was the sole grant => BPA denies.
    !granted_without_public
}

/// The effective BPA: a toggle is on if it is on at *either* account or bucket level.
fn effective_bpa(account: PublicAccessBlock, bucket: PublicAccessBlock) -> PublicAccessBlock {
    PublicAccessBlock {
        block_public_acls: account.block_public_acls || bucket.block_public_acls,
        ignore_public_acls: account.ignore_public_acls || bucket.ignore_public_acls,
        block_public_policy: account.block_public_policy || bucket.block_public_policy,
        restrict_public_buckets: account.restrict_public_buckets || bucket.restrict_public_buckets,
    }
}

/// Whether any explicit `Deny` statement in the bucket policy matches the request.
fn explicit_deny_matches(input: &AuthzInput) -> bool {
    let Some(policy) = &input.policy else {
        return false;
    };
    policy
        .statements
        .iter()
        .filter(|s| s.effect == Effect::Deny)
        .any(|s| statement_matches(s, input, true, true))
}

/// Whether any `Allow` statement matches, restricting which principal forms count.
///
/// * `allow_public_principal`: count statements whose principal is the `*` wildcard.
/// * `allow_user_principal`: count statements naming the requester's user id.
fn policy_allow_matches_scoped(
    input: &AuthzInput,
    allow_public_principal: bool,
    allow_user_principal: bool,
) -> bool {
    let Some(policy) = &input.policy else {
        return false;
    };
    policy
        .statements
        .iter()
        .filter(|s| s.effect == Effect::Allow)
        .any(|s| statement_matches(s, input, allow_public_principal, allow_user_principal))
}

/// Whether any `Deny` statement in the requester's attached identity (per-user) policy matches.
fn user_policy_deny_matches(input: &AuthzInput) -> bool {
    user_policy_matches(input, Effect::Deny)
}

/// Whether any `Allow` statement in the requester's attached identity (per-user) policy matches.
fn user_policy_allow_matches(input: &AuthzInput) -> bool {
    user_policy_matches(input, Effect::Allow)
}

/// Whether any statement of `effect` in the identity policy matches. Identity-policy statements are
/// matched WITHOUT a principal check — the requester is implicitly the principal.
fn user_policy_matches(input: &AuthzInput, effect: Effect) -> bool {
    let Some(policy) = &input.user_policy else {
        return false;
    };
    policy
        .statements
        .iter()
        .filter(|s| s.effect == effect)
        .any(|s| statement_matches_no_principal(s, input))
}

/// Like [`statement_matches`] but skips the principal gate, for identity (per-user) policy
/// statements where the requester is implicitly the principal.
fn statement_matches_no_principal(s: &Statement, input: &AuthzInput) -> bool {
    if !s
        .actions
        .iter()
        .any(|a| action_pattern_matches(a, input.action))
    {
        return false;
    }
    if !s
        .resources
        .iter()
        .any(|r| matching::resource_matches(r, &input.resource))
    {
        return false;
    }
    conditions_match(&s.conditions, &input.context, &input.requester)
}

/// Whether a single statement matches the request: principal, action, resource, conditions.
///
/// `allow_public_principal` / `allow_user_principal` gate which principal forms are counted,
/// used by the BPA computation to drop public-principal statements.
fn statement_matches(
    s: &Statement,
    input: &AuthzInput,
    allow_public_principal: bool,
    allow_user_principal: bool,
) -> bool {
    if !principal_matches(
        &s.principals,
        &input.requester,
        allow_public_principal,
        allow_user_principal,
    ) {
        return false;
    }
    if !s
        .actions
        .iter()
        .any(|a| action_pattern_matches(a, input.action))
    {
        return false;
    }
    if !s
        .resources
        .iter()
        .any(|r| matching::resource_matches(r, &input.resource))
    {
        return false;
    }
    conditions_match(&s.conditions, &input.context, &input.requester)
}

/// Whether a [`PrincipalSpec`] matches the requester, honouring the principal-form gates.
fn principal_matches(
    spec: &PrincipalSpec,
    requester: &RequesterClass,
    allow_public_principal: bool,
    allow_user_principal: bool,
) -> bool {
    match spec {
        PrincipalSpec::Any => allow_public_principal,
        PrincipalSpec::Users(ids) => {
            if !allow_user_principal {
                return false;
            }
            match requester {
                // Owner/admin is handled before policy allow evaluation, but a named statement
                // can still reference them (e.g. for the explicit-deny path).
                RequesterClass::OwnerOrAdmin => false,
                RequesterClass::AuthenticatedMember(uid) => ids.contains(uid),
                RequesterClass::Anonymous => false,
            }
        }
    }
}

/// Whether an [`ActionPattern`] matches an [`Action`].
fn action_pattern_matches(pattern: &ActionPattern, action: Action) -> bool {
    let name = action.as_s3_name();
    match pattern {
        ActionPattern::All => true,
        ActionPattern::Exact(p) => p == name,
        ActionPattern::Prefix(prefix) => name.starts_with(prefix.as_str()),
    }
}

/// Whether the requester is granted the needed permission by an ACL, given the ownership mode.
///
/// In [`OwnershipMode::BucketOwnerEnforced`], ACLs are disabled entirely and never grant.
/// `allow_public_grantees` is `false` when BPA neutralises public (group) ACL grants.
fn acl_allows_scoped(input: &AuthzInput, allow_public_grantees: bool) -> bool {
    if input.ownership_mode == OwnershipMode::BucketOwnerEnforced {
        return false;
    }

    // Object actions consult the object ACL; bucket actions consult the bucket ACL.
    let acl = match &input.resource {
        Resource::Object { .. } => input.object_acl.as_ref().or(input.bucket_acl.as_ref()),
        Resource::Bucket(_) => input.bucket_acl.as_ref(),
    };
    let Some(acl) = acl else {
        return false;
    };

    acl_grants(
        acl,
        &input.requester,
        input.action,
        &input.resource,
        allow_public_grantees,
    )
}

/// Whether `acl` contains a grant of the needed permission to the requester.
fn acl_grants(
    acl: &Acl,
    requester: &RequesterClass,
    action: Action,
    resource: &Resource,
    allow_public_grantees: bool,
) -> bool {
    acl.grants.iter().any(|g| {
        grantee_matches(&g.grantee, requester, &acl.owner, allow_public_grantees)
            && permission_satisfies(g.permission, action, resource)
    })
}

/// Whether an ACL grantee applies to the requester.
///
/// `AllUsers` applies to anyone (including anonymous); `AuthenticatedUsers` applies to any
/// authenticated requester; a `User` grantee applies to that specific user. `LogDelivery`
/// is a service group that never matches an ordinary requester here. The two group grantees
/// are "public" and are gated by `allow_public_grantees` (set false when BPA ignores them).
fn grantee_matches(
    grantee: &Grantee,
    requester: &RequesterClass,
    owner: &UserId,
    allow_public_grantees: bool,
) -> bool {
    match grantee {
        Grantee::AllUsers => allow_public_grantees,
        Grantee::AuthenticatedUsers => {
            allow_public_grantees && requester_is_authenticated(requester)
        }
        Grantee::LogDelivery => false,
        Grantee::User(uid) => match requester {
            RequesterClass::AuthenticatedMember(req) => req == uid,
            // An owner/admin is short-circuited earlier; but a user grant to the owner is
            // honoured for completeness.
            RequesterClass::OwnerOrAdmin => uid == owner,
            RequesterClass::Anonymous => false,
        },
    }
}

fn requester_is_authenticated(requester: &RequesterClass) -> bool {
    matches!(
        requester,
        RequesterClass::AuthenticatedMember(_) | RequesterClass::OwnerOrAdmin
    )
}

/// Whether the given policy (already parsed) contains any statement that grants public access
/// (a `*`-principal `Allow`). Exposed for the management surface's BPA warnings.
#[must_use]
pub fn policy_grants_public(policy: &Policy) -> bool {
    policy
        .statements
        .iter()
        .any(|s| s.effect == Effect::Allow && matches!(s.principals, PrincipalSpec::Any))
}

#[cfg(test)]
mod tests;
