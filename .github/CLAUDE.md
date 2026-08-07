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
| `stage-image` | `packages: write` | checksum-verified OCI archive and recorded subject digest |
| `sign-image` | OIDC, attestation, package write | run-unique candidate tag plus image-build SLSA predicate |
| `sign-assets` | OIDC and attestation | checksum manifest plus binary-build SLSA predicates and image binding |
| `publish-release` | `contents: write` | complete signed bundle; only an exact-commit draft or byte-identical published handoff is recoverable |
| `promote-latest` | `packages: write` | signed immutable digest plus successfully published release |
| `retire-prior-releases` | `contents: write` | successfully promoted version/latest image tags |

Build jobs never receive repository/package write or OIDC authority. Privileged jobs do not check
out source, compile, or execute artifact content. They compare fixed metadata against
`github.sha`, the once-computed CalVer, and the expected image name; verify SHA-256 manifests; and
validate the SLSA v1 predicates produced by the unprivileged build jobs before attesting those
exact subjects.

Release mutation is one concurrency group with `cancel-in-progress: false`, so a newer dispatch
never interrupts an in-flight run that may already have mutated one side of the release. GitHub
retains at most one pending run in a concurrency group and may replace an older pending dispatch;
operators therefore dispatch only when the group has no pending run. `stage-image` first copies the verified OCI archive
only to `candidate-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT`; signing and attestation target its immutable
digest, never `:latest`. `publish-release` refuses an orphan/conflicting tag or release/draft bound
to another commit. It may recover only a same-tag draft whose target and optional lightweight tag
resolve to the exact `github.sha`; checked deletion removes that tag first and the draft second so
every intermediate recovery state is rerunnable. It then uploads all assets into a new draft,
verifies the complete draft asset set, publishes it, and asserts that its lightweight tag resolves
to the exact commit. If publication succeeded but its response/ref assertion failed, a rerun may
resume only after the published non-prerelease, lightweight tag, exact target, complete asset-name
set, and every downloaded asset byte match the current signed bundle. Only then may
`promote-latest` verify the keyless image signature and point the CalVer and `:latest` tags at that
digest. Before either recovery or publication, the job paginates releases and matching tag refs and
refuses to continue if any exact CalVer name is newer than the current tag, preventing an older
failed run from promoting `:latest` backward.
`retire-prior-releases` runs last, independently paginates releases and Git refs, deletes only exact
older `vYYYY.MM.DD` names, verifies both sets again, and fails on every unexpected error. Unrelated
and later-dated releases and tags are untouched. A failed cross-service handoff can therefore leave
a valid signed immutable release/candidate for diagnosis, but can never advance `:latest` to an
unsigned or older image or silently claim that older releases were retired.

Before signing assets, `sign-assets` downloads both original `binaries` artifacts independently of
the assembly artifact. It rechecks each checksum, commit/version/target tuple, and builder-bound
predicate; byte-compares the assembled binaries and predicates to those originals; regenerates and
verifies the exact two-entry `SHA256SUMS`; and requires the image predicate's two binary-subject
hashes to equal those exact bytes. Every check precedes both GitHub attestations and Cosign blob
signatures. Every privileged provenance consumer also requires the exact build type, full builder
identity, exact source/dependency shape, and an invocation under the current workflow run whose
positive attempt number is no newer than the current rerun.

## Immutable automation inputs

- Pin every non-local `uses:` reference to a full 40-character commit, including GitHub-authored
  actions. For an annotated tag, pin the peeled `^{}` commit rather than the tag-object SHA.
- Pin Node, Python, Rust, cargo tools, Syft, Cosign, ORAS, Buildx, and Zig to exact versions.
  Downloaded wheels/archives require a checked-in SHA-256; install-action must keep
  `checksum: true` and `fallback: none`.
- Pin every Docker base and BuildKit driver image to a full `sha256:` digest. Do not add a mutable
  `# syntax=docker/dockerfile:<tag>` frontend.
- Never interpolate `${{ ... }}` directly into a shell `run:` script. Map each GitHub context,
  matrix, input, or output value through the step's `env:` block and quote its shell expansion.
  Shell quotes around the expression itself do not protect the pre-shell expression substitution.
- Keep workflow shell steps in the policy's canonical YAML subset: plain `run:` keys, plain
  single-line scripts or block scalars, and no flow-style step mappings, quoted keys, multiline
  inline scalars, tags, anchors, or aliases. The standard-library policy self-tests these evasions
  and fails closed on unsupported forms.
- Release dispatch has a job-level `github.ref == 'refs/heads/main'` gate before any step runs;
  retain the quoted shell check as defense in depth after context values cross through `env:`.
- Update an action/tool/image only by resolving its new identifier from the authoritative upstream,
  recording its immutable commit/digest/checksum, and validating the complete handoff again.

Run `python3 tests/release_policy.py` after every automation change. It is standard-library-only,
runs in CI before package installation, and fails mutable refs/images/tool selectors, unexpected
release jobs or permissions, authority in build jobs, unchecked or over-broad release deletion,
and missing verify-before-mutate ordering. It also pins the serialized candidate → sign → draft
release → version/latest promotion → retirement dependency chain, exact-commit draft recovery,
byte-identical published-release resumption, exact same-run provenance, direct
binary/predicate/image binding, older-CalVer-only ref retirement, and fatal deletion errors. It also
rejects direct GitHub expression interpolation in every workflow shell script and rejects YAML
forms that could hide a script from that check. Run `actionlint` (with a checksum-verified pinned
binary) when changing workflow structure too.

GitHub-hosted runners are the irreducible non-content-addressable trust root. Keep the explicit
`ubuntu-24.04` label (never `ubuntu-latest`) and checksum downloaded high-impact tools; `curl`,
`tar`, `sha256sum`, `jq`, the shell, Git/rustup, compiler/linker, and Docker remain runner-provided
bootstrap/system tools. Fully hermetic release requirements need a separately hardened immutable
self-hosted runner, without weakening any permission or digest-handoff rule above.
