# Governance & project commitments

Cairn is open-source infrastructure meant to hold production data. The most common reason teams
abandon a self-hosted object store is not a missing feature — it is a **trust break**: a project that
strips features out of the open edition, relicenses, or disappears. This document states what Cairn
is, how it is maintained, and the commitments that make it safe to build on.

## License is permanent

- Cairn is licensed under **Apache-2.0** ([`LICENSE`](./LICENSE)). Apache-2.0 is **irrevocable**:
  every commit ever published under it stays usable under those terms forever. A future maintainer
  cannot retroactively close source you already received.
- There is **no "enterprise edition"** and no open-core split. The features documented here and in
  [`docs/`](./docs) — Object Lock/WORM, versioning, multipart, presigned URLs, IAM/policies/ACL,
  SSE-S3, webhooks, replication, the admin console, metrics, audit — are the product, in the open
  repository. We will not move an existing open-source feature behind a paywall.
- If the project is ever relicensed going forward, it will be to another OSI-approved license and
  will never revoke the Apache-2.0 grant on already-published code.

## Maintenance model

- Cairn is **pre-1.0** and maintainer-led. Development happens in the open on
  [GitHub](https://github.com/Harsh-2002/Cairn); `main` is always the source of truth.
- Every change must pass the full CI gate before merge (see [`CONTRIBUTING.md`](./CONTRIBUTING.md)
  and [`docs/delivery.md`](./docs/delivery.md) §31). Security and data-durability fixes take priority.
- Releases are **date-based** (`ddmmyy`) and CI-gated: a release is only cut from a commit whose CI is
  green, and exactly one release is active at a time. See [Releases & verification](#releases--verification).
- Decisions, bugs, and feature requests are tracked as GitHub issues. There is no private roadmap that
  overrides the public one.

## Stability & deprecation

- The **on-disk format is forward-stable**: schema migrations are append-only and never rewrite an
  applied migration, so upgrading never strips data (see [`docs/upgrade-rollback.md`](./docs/upgrade-rollback.md)).
- The S3 API surface Cairn implements is documented honestly in
  [`docs/s3-api-matrix.md`](./docs/s3-api-matrix.md), including what is **deliberately out of scope** —
  we would rather decline a feature clearly than ship a silent stub.
- Pre-1.0, the management API and configuration knobs may change between releases; such changes are
  called out in the release notes.

## Security

- Report vulnerabilities privately per [`SECURITY.md`](./SECURITY.md) — do **not** open a public
  issue for an unpatched vulnerability. Crypto fails closed; secrets are never logged or returned.

## Releases & verification

Releases publish two artifacts, both reproducible from the released commit:

- **Static binaries** (`linux/amd64`, `linux/arm64`, musl, fully static) attached to the
  [GitHub Release](https://github.com/Harsh-2002/Cairn/releases), with a `SHA256SUMS` manifest.
- A **multi-arch container image** on **GHCR** (a registry the project controls):
  `ghcr.io/harsh-2002/cairn:latest`, built from those exact binaries on a distroless, non-root base.

Verify a downloaded binary against the published manifest:

```sh
# from the directory containing the downloaded assets + SHA256SUMS
sha256sum -c SHA256SUMS
```

Pull and inspect the image:

```sh
docker pull ghcr.io/harsh-2002/cairn:latest
docker image inspect ghcr.io/harsh-2002/cairn:latest --format '{{.Architecture}} {{.Os}}'
```

> Artifact **signing** (cosign/Sigstore) and SBOM attestation are planned hardening, not yet in the
> release pipeline; the `SHA256SUMS` manifest is the current integrity anchor.

## Community

Participation is governed by the [Code of Conduct](./CODE_OF_CONDUCT.md).
