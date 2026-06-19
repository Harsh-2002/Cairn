# cairn-authz

The **pure** authorization engine (`AuthorizationEngine`): policy / ACL / public-access-block /
object-ownership evaluation, with no I/O.

## Layout (`src/`)
- `lib.rs` — `PolicyEngine` and the fixed evaluation order (the owner/admin short-circuit, then policy).
- `parse.rs` — bucket/identity policy parsing (the `parse_policy` fuzz target).
- `condition.rs` — policy condition operators. `acl.rs` — ACL grants. `matching.rs` — ARN/action/resource matching.
- `tests.rs` — the matrix + property tests for evaluation order.

## Notes
- Pure: inputs in, decision out — never reads the store or the network.
- An object with **no ACL is private** (it does NOT fall back to the bucket ACL — audit #2).
- Spec: `docs/auth.md` (15). See the root `../../CLAUDE.md`.
