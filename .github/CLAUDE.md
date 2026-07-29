# GitHub automation — scoped agent guidance

Read root [`CLAUDE.md`](../CLAUDE.md), [`CONTRACT.md`](../CONTRACT.md), and delivery ARCH 31 before
changing this directory. A release remains a deliberate human `workflow_dispatch`; agents must not
tag, publish, dispatch, delete, or retire a release.

## Release trust boundary

`release.yml` has no workflow-wide permission. Its job permissions are an allow-list:

| Job | Authority | Required input |
|---|---|---|
| `verify-ci` | `actions: read` | exact `github.sha` CI result |
| `binaries`, `release-assets`, `image-build` | read-only or none | repository source and pinned tools |
| `publish-image` | `packages: write` | checksum-verified OCI archive and recorded subject digest |
| `sign-image` | OIDC, attestation, package write | published digest plus image-build SLSA predicate |
| `sign-assets` | OIDC and attestation | checksum manifest plus binary-build SLSA predicates and image binding |
| `publish-release` | `contents: write` | complete signed-bundle manifest |

Build jobs never receive repository/package write or OIDC authority. Privileged jobs do not check
out source, compile, or execute artifact content. They compare fixed metadata against
`github.sha`, the once-computed CalVer, and the expected image name; verify SHA-256 manifests; and
validate the SLSA v1 predicates produced by the unprivileged build jobs before attesting those
exact subjects. Only then may they publish, sign, attest, or mutate a GitHub Release. Release/tag
deletion belongs only to the final `publish-release` job.

## Immutable automation inputs

- Pin every non-local `uses:` reference to a full 40-character commit, including GitHub-authored
  actions. For an annotated tag, pin the peeled `^{}` commit rather than the tag-object SHA.
- Pin Node, Python, Rust, cargo tools, Syft, Cosign, ORAS, Buildx, and Zig to exact versions.
  Downloaded wheels/archives require a checked-in SHA-256; install-action must keep
  `checksum: true` and `fallback: none`.
- Pin every Docker base and BuildKit driver image to a full `sha256:` digest. Do not add a mutable
  `# syntax=docker/dockerfile:<tag>` frontend.
- Update an action/tool/image only by resolving its new identifier from the authoritative upstream,
  recording its immutable commit/digest/checksum, and validating the complete handoff again.

Run `python3 tests/release_policy.py` after every automation change. It is standard-library-only,
runs in CI before package installation, and fails mutable refs/images/tool selectors, unexpected
release jobs or permissions, authority in build jobs, release deletion outside the final job, and
missing verify-before-mutate ordering. Also run `actionlint` (with a checksum-verified pinned
binary) when changing workflow structure.

GitHub-hosted runners are the irreducible non-content-addressable trust root. Keep the explicit
`ubuntu-24.04` label (never `ubuntu-latest`) and checksum downloaded high-impact tools; `curl`,
`tar`, `sha256sum`, `jq`, the shell, Git/rustup, compiler/linker, and Docker remain runner-provided
bootstrap/system tools. Fully hermetic release requirements need a separately hardened immutable
self-hosted runner, without weakening any permission or digest-handoff rule above.
