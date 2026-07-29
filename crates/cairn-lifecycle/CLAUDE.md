# cairn-lifecycle

The background lifecycle scanner (ARCH 19): parses an S3 `<LifecycleConfiguration>` into typed
rules and applies the due ones over the trait spine — expiration, noncurrent-version expiration,
expired-object-delete-marker removal, and abort-incomplete-multipart.

## Layout (`src/`)
- `config.rs` — `parse_lifecycle` (XML → `Vec<LifecycleRule>`), the `Action`/`Expiration`/`Filter`
  types, and a SAX-style `drive` over quick-xml. Total function: any malformed body folds to
  `Error::MalformedXml`, never a panic. The driver coalesces text/CDATA/reference events and accepts
  only numeric references plus XML's predefined entities.
- `scanner.rs` — `LifecycleScanner::run_once` (the periodic apply pass) and `LifecycleReport`.
- `lib.rs` re-exports; `tests.rs` is the whole suite (against the in-memory doubles).

## Notes
- **Trait-spine only.** The scanner touches the world solely through `MetadataStore` / `BlobStore`
  / `Clock` passed to `run_once` — it owns no state (`LifecycleScanner` is a ZST) and opens no
  connections. Same code drives the SQLite/FS backends in prod and the in-memory doubles in tests;
  keep new logic backend-agnostic.
- **Idempotent — this is the contract** (ARCH 19.2). Every action is a convergent state transition,
  so a rerun or interrupted scan reaches the same end state. Current-object expiration in a
  versioned bucket relies on `list_current` excluding delete markers, so the inserted marker hides
  the key and no second marker is added. NEVER add an action that isn't a no-op once applied.
- Incomplete-upload abort obeys the multipart terminal-owner outcome: reclaim staged parts only
  after the writer returns `Aborted`. `NotOwner` means a concurrent Complete owns `completing`; it is
  an expected skip, and deleting that session's bytes would corrupt the winning completion.
- **Versioned vs not.** Expiring a current object in a versioning-*enabled* bucket inserts a delete
  marker (`CreateDeleteMarker`); unversioned/suspended permanently deletes and reclaims the blob.
- **Object Lock outranks expiry at the Writer.** Lifecycle passes one trusted `now` and
  `GovernanceBypass::Denied` with every permanent-delete or delete-marker mutation. The Writer
  evaluates the version's current retention/legal-hold state in the same savepoint as deletion and
  returns `DeleteProtected`; lifecycle silently skips that outcome (neither expired nor an error),
  and the rule applies once protection lapses. Never restore a read-then-delete lock check or grant
  lifecycle a governance bypass.
- Lifecycle reports count a permanent deletion only after the writer returns `Deleted`.
  `DeleteNotApplied` is an expected skip: the row disappeared or its `expected_updated_at` guard
  lost a concurrent overwrite race, so no metadata changed and no expiration counter advances.
- **Transition is rejected at write time**, not silently stored. A `PutBucketLifecycleConfiguration`
  carrying a `<Transition>` is refused with `NotImplemented` in `cairn-protocol` (`service.rs`,
  search `Action::Transition`). The variant is still *parsed* — so the write path can detect/reject
  and the scanner tolerates any pre-existing stored config — but the scanner does **no** data
  movement and does not count it (cold-tier transition, ARCH 19.5, is unimplemented).
- **Bounded enumeration only.** Page through `list_current` / `list_versions` (`PAGE_LIMIT = 1000`)
  and each bucket's `list_multipart_uploads` (`SESSION_BATCH`, using the full key/upload-id tuple
  cursor). Never take one global first-N MPU window: another bucket or a hot key can otherwise
  starve every later eligible session forever. Version scans consume the ordered listing one
  complete key group at a time, carrying only a key that crosses a page boundary; peak memory is one
  page plus the largest key's version history, never the bucket. `run_once` errors only if an
  enumeration itself fails — per-item mutation failures are tallied in `report.errors`, never
  fatal, so one bad item can't stall a bucket.
- **Lifecycle delete markers replicate.** `marker_replication` fans a versioned-expiry marker out
  to every matching replication target (1→N) via `cairn-replication`, exactly like a client delete
  (ARCH 20.2/20.3); this is the one cross-crate dependency.
- A `<Tag>` that closes without both `<Key>` and `<Value>` is malformed. `<Date>` accepts whole
  epoch seconds or RFC-3339 (a hand-rolled parser; no chrono).

## Wiring & spec
- Driven by `cairn-server`'s `lifecycle_loop` (`background.rs`) every `CAIRN_LIFECYCLE_INTERVAL_SECS`
  (default 3600, must be positive). The server collects each bucket's stored `Lifecycle` config doc
  and calls `run_once`.
- Spec: `docs/s3-api.md` 19 (19.1 config/filter, 19.2 scanner, 19.3 expiration, 19.4 abort,
  19.5 transition). See the root `../../CLAUDE.md` for the gate and workspace invariants.
