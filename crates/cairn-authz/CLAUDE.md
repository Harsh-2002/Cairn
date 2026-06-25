# cairn-authz

The **pure** authorization engine: policy / ACL / Block-Public-Access / object-ownership
evaluation. Inputs in (`AuthzInput`), a `Decision` out — **no I/O, no store, no clock, no
network**. Depends only on `cairn-types`; the caller assembles the `AuthzInput`.

## Layout (`src/`)
- `lib.rs` — `PolicyEngine` (the `cairn_types::AuthorizationEngine` impl) + the free `evaluate`
  fn. Owns the **fixed evaluation order** and the BPA gate (`block_public_access_denies`).
- `parse.rs` — `parse_policy` (bucket policy, principal required) and `parse_user_policy`
  (identity/per-user policy, principal-less). The `parse_policy` fuzz target. Bad shapes →
  `Error::MalformedPolicy`.
- `condition.rs` — condition operators + key resolution against `RequestContext`.
- `acl.rs` — `expand_canned_acl` (canned-name → `Acl`) and `permission_satisfies`
  (`Permission` → which `Action`s it grants).
- `matching.rs` — ARN rendering + `wildcard_match` (`*`/`?` glob; iterative, never recurses).
- `tests.rs` — the decision matrix + property tests for evaluation order.

## Invariants (get these right)
- **Evaluation order is fixed** (ARCH 15.3) and MUST NOT be reordered:
  (a) owner/admin → Allow **unless** an explicit Deny matches (bucket policy OR the requester's
  own identity policy — an identity Deny binds even the owner); (b) BPA gate; (c) explicit Deny
  anywhere → Deny; (d) any Allow (bucket policy, identity policy, or ACL); (e) default Deny.
- **Fail-closed by default.** An unrecognised condition key or operator makes the statement
  **not match** — an unknown condition can never broaden access. Default outcome is Deny.
- **An object with no ACL is private** — it does NOT fall back to the bucket ACL (audit #2).
  Object actions consult `object_acl`; bucket actions consult `bucket_acl`; never cross them.
- **`BucketOwnerEnforced` disables ACLs entirely** — `acl_allows_scoped` returns false; only
  policy can grant.
- **BPA denies only when public was the *sole* grant.** The gate computes "granted with public
  grants" vs. "granted with them stripped"; an identity/user-policy grant is never public and
  always survives. Effective BPA is the **OR** of account + bucket toggles (stricter wins).
- **Identity-policy statements are matched WITHOUT a principal check** — the requester is
  implicitly the principal (`statement_matches_no_principal`). `parse_user_policy` stores an
  omitted principal as `PrincipalSpec::Any`, but the value is never consulted.
- `NotPrincipal`/`NotUsers` Allow counts as **public** (`policy_grants_public`) so BPA governs
  it exactly like a `"*"` Allow.

## Notes
- Pure & stateless: `PolicyEngine` is `Copy + Default`; construct once and share. Prefer the
  free `evaluate` to avoid a trait object.
- ACL semantics live in `permission_satisfies` (15.7): `Read` on an object → `GetObject*`, on a
  bucket → `ListBucket*`; `Write` → object create/overwrite/delete + object-subresource writes;
  `*Acp` → `Get*Acl`/`Put*Acl`; `FullControl` → everything. Add a new `Action` to the matching
  arm here or ACL grants silently won't cover it.
- A new condition key/operator must be handled in `condition.rs` **and** parse-accepted in
  `parse.rs`, or policies using it fail to parse / silently don't match.
- Spec: `docs/auth.md` 15 (15.3 order, 15.5 policy language, 15.6 conditions, 15.7 ACLs).
  Sibling `cairn-auth` does authentication (SigV4/Bearer) and builds the `AuthzInput`.
  See the root `../../CLAUDE.md`.
