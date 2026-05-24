# admin E2E tests

Playwright smoke + integration suite for the Cairn admin SPA.

## Run

```sh
# 1. Install browsers (first time only)
npx playwright install chromium

# 2. Start the backend in another terminal:
cargo run -p cairn -- serve <some-source-dir> --admin-secret test-secret

# 3. Start the Vite dev server in a third terminal:
npm --prefix admin run dev

# 4. Run the suite
npm --prefix admin exec playwright test
```

Against the embedded production SPA (no Vite required):

```sh
cargo build --release -p cairn
./target/release/cairn serve <source> --admin-secret test
CAIRN_BASE_URL=http://localhost:8080 npm --prefix admin exec playwright test
```

## What's covered today

- `smoke.spec.ts` — the SPA loads, the header renders, the bundle has no
  unexpected JS errors on mount.

## What's still TODO (Phase 4 polish)

- A spec that drives the editor: type, autosave fires, draft branch
  appears in a `MockGitHubServer` (per the testing plan in
  `~/.claude/plans/`).
- A two-tab race spec verifying each tab gets its own session UUID
  branch.
- Image paste → presigned PUT → markdown reference flow with a real
  MinIO container as the bucket.
- Slug-collision-fails-closed spec.
- Cold-reload-restores-from-drafts-branch spec.

These need a richer harness (`MockGitHubServer` via wiremock-rs spun up
from a Rust integration test, plus `testcontainers` MinIO) which is
beyond the Phase 3.5 minimum.
