# Cairn GitHub Action

Build a Cairn blog without installing a Rust toolchain.

## Usage

```yaml
- uses: anthropics/cairn/.github/action@main
  with:
    source: .
    output: _site
- uses: cloudflare/wrangler-action@v3
  with:
    apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
    command: pages deploy _site --project-name=my-blog
```

The action is a Docker action; the image is built from the repository's `cli/`
and `crates/` at the pinned revision. For the published Action distribution
(after Phase 6 stabilizes the surface) a separate `cairn-action` repository
will host pre-built images.

## Determinism

Same input commit → same output bytes. This action is one half of the
contract; the other half is the Determinism workflow in this repository
that runs the build on three architectures and compares hashes.
