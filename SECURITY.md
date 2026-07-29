# Security policy

Cairn is an S3-compatible object-storage server that holds production data and secrets, so security
reports are taken seriously.

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue for an unpatched
vulnerability. Use GitHub's private vulnerability reporting on this repository
(**Security → Report a vulnerability**).

Include the affected version/commit, a description, reproduction steps, and the impact. We aim to
acknowledge reports promptly and to coordinate a fix and disclosure.

## Security model

The architecture and threat model are documented in
[`docs/security-errors.md`](./docs/security-errors.md) (sections 25, 27). Load-bearing invariants:

- Secrets at rest — SigV4 secrets, replication credentials, and SSE-S3 data keys — are
  AES-256-GCM envelope-encrypted; **reads fail closed** (a missing/wrong key errors, never returns
  plaintext) and secrets are never logged, echoed, or returned by any endpoint.
- The S3 authorization pipeline (policy / ACL / public-access-block / object-ownership) is
  fail-closed; an object with no ACL is private.
- Master-key rotation and the retire-gate are described in
  [`docs/operations.md`](./docs/operations.md).

## Verifying release artifacts

Every release is signed and comes with provenance. The binaries and `SHA256SUMS` are signed with
[cosign](https://docs.sigstore.dev/) **keyless** (Sigstore OIDC — no long-lived key), an SPDX
dependency SBOM (`cairn-sbom.spdx.json`) is attached, and both the binaries and the container image
carry SLSA **build-provenance attestations**. The no-authority build jobs emit the SLSA v1
predicates; isolated attestation jobs consume them only after checking the subject, commit,
version, target/image and registry-digest bindings. `PUBLISH-MANIFEST.sha256` covers every
published binary, signature bundle, checksum file, SBOM, provenance predicate, and `IMAGE-DIGEST`;
verify it before selecting a subject.

The workflow gives compilation and OCI assembly no package/repository write or OIDC permission.
Isolated publishing and signing jobs verify the build job's SHA-256, commit, CalVer, target, and
image-digest bindings before using their one authority. All actions, tool installers,
SBOM/signing tools, BuildKit inputs, and container bases are immutable commit/version/checksum or
digest pins, enforced by `python3 tests/release_policy.py`. GitHub-hosted runner images are not
content-addressable; the workflow bounds that platform trust to the `ubuntu-24.04` label and uses
its bootstrap/system tools, while checksum-verifying every downloaded high-impact release tool.

The signing identity is this repository's release workflow, and the issuer is GitHub's OIDC
provider. Substituting the identity/issuer below with anything else means the artifact was not built
by this pipeline.

```sh
IDENTITY='https://github.com/Harsh-2002/Cairn/.github/workflows/release.yml@refs/heads/main'
ISSUER='https://token.actions.githubusercontent.com'

# Verify the release bundle's fixed subjects first:
sha256sum -c PUBLISH-MANIFEST.sha256

# Binary (and SHA256SUMS) — verify the detached cosign bundle attached to the release:
cosign verify-blob \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" \
  --bundle cairn-linux-amd64.cosign.bundle \
  cairn-linux-amd64

# Container image — use the digest attached to this release, never the mutable tag as the subject:
IMAGE='ghcr.io/harsh-2002/cairn'
DIGEST="$(cat IMAGE-DIGEST)"
printf '%s\n' "$DIGEST" | grep -Eq '^sha256:[0-9a-f]{64}$'
cosign verify \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" \
  "${IMAGE}@${DIGEST}"

# Build provenance (binary or image) via the GitHub CLI:
gh attestation verify cairn-linux-amd64 --repo Harsh-2002/Cairn
gh attestation verify "oci://${IMAGE}@${DIGEST}" --repo Harsh-2002/Cairn
```

## Supported versions

Cairn is pre-1.0; security fixes land on `main`.
