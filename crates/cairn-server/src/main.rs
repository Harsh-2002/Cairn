//! The Cairn server binary entrypoint. Parses configuration, initialises observability, builds
//! the engine stack, and runs the HTTP server with ordered graceful shutdown. Also carries the
//! node-local commands that operate directly on the data dir from config: `bootstrap` (mint the
//! first administrator), `integrity` (on-demand reconciliation), `migrate` (run migrations and
//! report the schema version), and `backup`/`restore` (the ARCH 31.4 consistent snapshot and its
//! inverse). The full remote-admin CLI ships as `cairn-cli` in a later wave.

// The default (and every non-`fast-io`) build keeps the strongest posture: `forbid(unsafe_code)`
// makes it impossible to introduce `unsafe` anywhere in the crate. The experimental, Linux-only
// `fast-io` performance path needs a few raw syscalls (kTLS setsockopt probe, `sendfile(2)`), so
// under that feature we relax to `deny(unsafe_code)` — still rejecting every `unsafe` block by
// default, but allowing the individually reviewed, SAFETY-commented blocks in `sendfile.rs` to
// opt in with `#[allow(unsafe_code)]`. `forbid` cannot be locally overridden; `deny` can.
#![cfg_attr(not(feature = "fast-io"), forbid(unsafe_code))]
#![cfg_attr(feature = "fast-io", deny(unsafe_code))]

mod adapter;
mod background;
mod cli_remote;
mod config;
mod import_dest;
mod import_run;
mod key_rewrap;
mod metrics_agg;
mod observability;
mod replication_audit;
mod server;
mod sse;
mod sts;
// Linux-only zero-copy syscall helpers for the `fast-io` perf path (kTLS probe + sendfile(2)).
// Gated to the feature *and* Linux so it is absent (and cannot warn) in every other build.
#[cfg(all(feature = "fast-io", target_os = "linux"))]
mod sendfile;
// The plaintext HTTP/1.1 sendfile fast path for object GETs; same gate as `sendfile`.
#[cfg(all(feature = "fast-io", target_os = "linux"))]
mod fast_get;
mod stack;
mod tls;
mod update_check;

use clap::{Parser, Subcommand};
use config::Config;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

/// The user-facing version, baked in at build time (`build.rs::emit_version` → `$OUT_DIR/version.txt`):
/// the calendar release (`vYYYY.MM.DD`) for a release build, or `x.y.z-dev+gSHA` for a local build.
/// Surfaced by `cairn --version` and by `SystemInfo` (`GET /system`, the console footer).
pub(crate) const CAIRN_VERSION: &str = include_str!(concat!(env!("OUT_DIR"), "/version.txt"));

/// Cairn — a production-grade, S3-compatible object storage server. Configuration is
/// environment-only: set `CAIRN_*` variables (there is no configuration file).
#[derive(Debug, Parser)]
// `version` is the build-injected `CAIRN_VERSION` (the calendar release, or a `-dev` marker for a
// local build) — never the bare crate `CARGO_PKG_VERSION`. See `build.rs::emit_version`.
#[command(name = "cairn", version = CAIRN_VERSION, about)]
struct Cli {
    /// The subcommand to run (defaults to `serve`).
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the server.
    Serve,
    /// Validate the configuration and exit.
    ValidateConfig,
    /// Ensure the single root administrator exists and print its credentials. Idempotent, and the
    /// same identity `serve` seeds — so a node always has exactly one default admin (root).
    Bootstrap,
    /// Run reconciliation on demand (reclaim orphaned blobs); a node-local integrity check.
    ///
    /// With `--repair`, additionally run in repair mode (ARCH 24.3/29.4): drop metadata rows
    /// whose backing blob is missing on disk, so the store can re-serve the remaining keys cleanly.
    Integrity {
        /// Also drop metadata rows whose backing blob is missing (destructive repair).
        #[arg(long)]
        repair: bool,
    },
    /// Open the store (running migrations) and report the applied schema version.
    Migrate,
    /// Take a consistent snapshot of the data dir into DIR (ARCH 31.4): checkpoint + copy the
    /// database, then copy the blob tree excluding the staging area.
    Backup {
        /// Destination directory for the snapshot (created if absent).
        dir: PathBuf,
    },
    /// Restore a snapshot from DIR into the configured data dir, then run reconciliation
    /// (ARCH 31.4): place the database and blobs, then reconcile.
    Restore {
        /// Source snapshot directory produced by `backup`.
        dir: PathBuf,
    },

    // --- Remote administration (ARCH 24.2): a thin client over a running server's management API
    //     and S3 data plane. These commands do not touch the local data dir or config; they are
    //     dispatched before `Config::load()`. Connection + output options come from the flattened
    //     `RemoteOpts` (flags or `CAIRN_*` env).
    /// Bucket operations against a running server's management API.
    Bucket {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::BucketCmd,
    },
    /// User operations against a running server's management API.
    User {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::UserCmd,
    },
    /// Replication operations against a running server's management API.
    Replication {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::ReplicationCmd,
    },
    /// Object operations over a running server's S3 data plane (same Bearer token).
    Object {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::ObjectCmd,
    },
    /// Object sharing on a running server: share links + presigned URLs.
    Share {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::ShareCmd,
    },
    /// Import buckets + objects from another S3-compatible store into a running server.
    Import {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
        #[command(subcommand)]
        cmd: cli_remote::ImportCmd,
    },
    /// Print a running server's store overview.
    Overview {
        #[command(flatten)]
        opts: cli_remote::RemoteOpts,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Serve);

    // Remote-administration commands are a thin client over a running server's HTTP surfaces and
    // never read the local data dir or environment-only config; dispatch them before `Config::load`
    // so they work without a configured node (only `--endpoint`/`--access-key`/`--secret-key` or the
    // corresponding `CAIRN_*` vars matter).
    match command {
        Command::Bucket { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Bucket { cmd });
        }
        Command::User { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::User { cmd });
        }
        // `replication audit` is the one NODE-LOCAL replication subcommand: it reads the durable
        // version-row ledger (`object_versions.replication_status`), which no management API
        // exposes — deliberately, because the outbox the API *does* expose is pruned at
        // `CAIRN_REPLICATION_RETENTION_SECS` and would answer "all clear" for an incident that
        // predates the window. It falls through to `Config::load()` below; every other replication
        // subcommand is a thin remote client.
        Command::Replication { opts, cmd }
            if !matches!(cmd, cli_remote::ReplicationCmd::Audit { .. }) =>
        {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Replication { cmd });
        }
        Command::Object { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Object { cmd });
        }
        Command::Share { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Share { cmd });
        }
        Command::Import { opts, cmd } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Import { cmd });
        }
        Command::Overview { opts } => {
            return cli_remote::run(&opts, cli_remote::RemoteCommand::Overview);
        }
        _ => {}
    }

    // Node-local commands need the environment-only config.
    let cfg = match Config::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            return ExitCode::from(2);
        }
    };

    match command {
        Command::ValidateConfig => {
            // The fields parsed; also enforce the serve-time deployment guardrail so an operator who
            // runs `validate-config` before deploying is told about an insecure public bind.
            if let Err(e) = cfg.refuse_insecure_public_bind() {
                eprintln!("configuration error: {e}");
                return ExitCode::from(2);
            }
            println!("configuration valid");
            ExitCode::SUCCESS
        }
        Command::Bootstrap => bootstrap(cfg),
        Command::Integrity { repair } => integrity(cfg, repair),
        Command::Migrate => migrate(cfg),
        Command::Backup { dir } => backup(cfg, &dir),
        Command::Restore { dir } => restore(cfg, &dir),
        Command::Serve => {
            if let Err(e) = cfg.refuse_insecure_public_bind() {
                eprintln!("configuration error: {e}");
                return ExitCode::from(2);
            }
            run_server(cfg)
        }
        // The one node-local replication subcommand (see the dispatch guard above).
        Command::Replication {
            cmd:
                cli_remote::ReplicationCmd::Audit {
                    before,
                    bucket,
                    json,
                    verify,
                },
            ..
        } => replication_audit(cfg, before.as_deref(), bucket.as_deref(), json, verify),
        // The remote-admin variants are handled and returned above.
        Command::Bucket { .. }
        | Command::User { .. }
        | Command::Replication { .. }
        | Command::Object { .. }
        | Command::Share { .. }
        | Command::Import { .. }
        | Command::Overview { .. } => unreachable!("remote commands dispatched above"),
    }
}

fn integrity(cfg: Config, repair: bool) -> ExitCode {
    use cairn_types::blob::ReconcileOpts;
    use cairn_types::traits::BlobStore;

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async {
        // Open through the configured backend (CAIRN_META_BACKEND) so reconciliation consults the
        // same engine the server serves from. Repair mode needs the metadata store itself (to drop
        // dangling rows), so keep both halves.
        let (meta, oracle) = match stack::open_meta_store(&cfg).await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        let blob = match cairn_blob::LocalBlobStore::open(cfg.data_dir.clone()).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("failed to open blob store: {e}");
                return ExitCode::FAILURE;
            }
        };

        // First, the always-on forward pass: reclaim orphaned blobs (blobs with no metadata row).
        // `integrity` is an explicit, on-demand reconcile run against a quiesced store (no in-flight
        // writes), so reclaim crash-orphans immediately (margin 0) rather than honouring the live-
        // operation safety margin.
        let opts = ReconcileOpts {
            staging_safety_margin_secs: 0,
            ..ReconcileOpts::default()
        };
        match blob.reconcile(oracle.as_ref(), opts).await {
            Ok(r) => {
                println!(
                    "reconciliation complete: scanned={} orphans_reclaimed={} staging_cleaned={} sessions_cleaned={} errors={}",
                    r.blobs_scanned, r.orphans_reclaimed, r.staging_cleaned, r.sessions_cleaned, r.errors
                );
            }
            Err(e) => {
                eprintln!("reconciliation failed: {e}");
                return ExitCode::FAILURE;
            }
        }

        // Then, in repair mode, the inverse pass: drop metadata rows whose backing blob is missing
        // on disk (ARCH 24.3/29.4). The forward reconcile cannot detect these — it only walks the
        // blob tree — so repair walks the metadata instead, probes the blob store for each version's
        // backing object, and deletes the row when the blob is gone.
        if repair {
            match repair_dangling_rows(meta.as_ref(), &blob).await {
                Ok(dropped) => {
                    println!("repair complete: dangling_rows_dropped={dropped}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("repair failed: {e}");
                    ExitCode::FAILURE
                }
            }
        } else {
            ExitCode::SUCCESS
        }
    })
}

/// The page size used when walking metadata in repair mode; bounds memory per round.
const REPAIR_PAGE_LIMIT: u32 = 1000;
/// Upper bound on paging iterations per bucket, so a hostile/corrupt cursor can never spin forever.
const REPAIR_MAX_PAGES: u32 = 100_000;

/// Repair-mode reconciliation (ARCH 24.3/29.4): drop every metadata row whose backing blob is
/// missing on disk. Walks each bucket's versions, resolves each non-delete-marker version's
/// `storage_path`, probes the blob store for it, and submits a `DeleteVersion` mutation when the
/// blob is absent. Returns the count of rows dropped.
///
/// This composes only the public store/blob primitives (no privileged internals): it is the
/// node-local inverse of orphan reclamation and is deliberately destructive, so it runs only under
/// the explicit `--repair` flag.
async fn repair_dangling_rows(
    meta: &dyn cairn_types::traits::MetadataStore,
    blob: &cairn_blob::LocalBlobStore,
) -> Result<u64, String> {
    use cairn_types::error::BlobError;
    use cairn_types::meta::{ListQuery, Mutation, MutationOutcome};
    use cairn_types::traits::BlobStore;

    let buckets = meta.list_buckets(None).await.map_err(|e| e.to_string())?;
    let mut dropped = 0u64;

    for bucket in &buckets {
        let mut cursor: Option<String> = None;
        // A version page resumes on the (key, version-id) PAIR, so thread BOTH the boundary key and
        // its version-id marker back. Feeding only the key half re-lists a key that holds more
        // versions than one page at every boundary and, worst case, never terminates (issue #7).
        let mut vmarker: Option<String> = None;
        for _ in 0..REPAIR_MAX_PAGES {
            let query = ListQuery {
                cursor: cursor.clone(),
                version_id_marker: vmarker.clone(),
                limit: REPAIR_PAGE_LIMIT,
                ..Default::default()
            };
            let page = meta
                .list_versions(&bucket.name, &query)
                .await
                .map_err(|e| e.to_string())?;
            if page.items.is_empty() {
                break;
            }

            for item in &page.items {
                // Delete markers carry no blob, so they are never dangling.
                if item.is_delete_marker {
                    continue;
                }
                // Resolve the version's backing storage path. A row that has gone missing between
                // the listing and this read is simply skipped (nothing to repair).
                let row = match meta
                    .get_version(&bucket.name, &item.key, &item.version_id)
                    .await
                {
                    Ok(Some(r)) => r,
                    Ok(None) => continue,
                    Err(e) => return Err(e.to_string()),
                };
                let Some(path) = row.storage_path.clone() else {
                    continue;
                };

                // Probe the blob store for PRESENCE only (no body, no DEK, no decrypt): a present
                // blob — plaintext or encrypted — returns `Ok`; a missing blob yields `NotFound`,
                // exactly the dangling case we repair. Any other error is surfaced rather than
                // treated as "missing", so a transient I/O fault never deletes good metadata.
                match blob.probe(&path).await {
                    Ok(_present) => {}
                    Err(BlobError::NotFound) => {
                        match meta
                            .submit(Mutation::DeleteVersion {
                                bucket: bucket.name.clone(),
                                key: item.key.clone(),
                                version_id: item.version_id.clone(),
                                expected_updated_at: None,
                            })
                            .await
                        {
                            Ok(MutationOutcome::Deleted { freed, .. }) => {
                                // Best-effort, idempotent: the blob is already gone, but reclaim any
                                // path the store reports freed so no surprise orphan remains.
                                if let Some(freed) = freed {
                                    let _ = blob.delete(&freed).await;
                                }
                                dropped += 1;
                            }
                            Ok(_) => {}
                            Err(e) => return Err(e.to_string()),
                        }
                    }
                    Err(e) => return Err(e.to_string()),
                }
            }

            match page.next_cursor {
                Some(next) => {
                    cursor = Some(next);
                    vmarker = page.next_version_id_marker;
                }
                None => break,
            }
        }
    }

    Ok(dropped)
}

/// How many suspect versions per bucket the human-readable audit lists individually. Counts are
/// always exact; only this sample is bounded, so a bucket with a million suspects still prints.
const AUDIT_SAMPLE_LIMIT: usize = 20;

/// The largest replica body `--verify` will **read** for the byte comparison. Matches the sink's
/// PUT buffer cap: an object too large to have been replicated is also too large to verify.
///
/// This is a total-bytes-read bound, not a buffer size — the body is hashed frame by frame and
/// never held (`HttpS3Sink::stream_object`), so verifying a 2 GiB replica costs O(1) memory. The cap
/// survives anyway, because a hostile or misconfigured destination that streams without end must
/// still terminate the check rather than run until the operator kills it.
const AUDIT_VERIFY_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// `cairn replication audit --before TS [--bucket B] [--json] [--verify]` (ARCH 20.5, 26.4).
///
/// Reports the object versions that are **encrypted**, **terminally replicated**, and **created
/// before the cutoff** — the population left behind by the pre-release-X replication defect that
/// shipped SSE objects as raw ciphertext. See `replication_audit.rs` for why this reads the
/// version-row ledger rather than the outbox, why the cutoff is mandatory, and why a remote
/// size/ETag comparison would be worthless.
fn replication_audit(
    cfg: Config,
    before: Option<&str>,
    bucket: Option<&str>,
    json: bool,
    verify: bool,
) -> ExitCode {
    // The cutoff is required. `--before` wins; `CAIRN_REPLICATION_AUDIT_BEFORE` is the fallback so
    // an operator who configured the gauge does not have to retype it. There is deliberately NO
    // implicit default: guessing a cutoff would silently change what every number in this report
    // means, and "now" in particular would report every healthy encrypted replica as suspect.
    let raw = match before.or(cfg.replication_audit_before.as_deref()) {
        Some(v) => v,
        None => {
            eprintln!(
                "replication audit needs a cutoff: pass --before <RFC3339|epoch-seconds>, or set \
                 CAIRN_REPLICATION_AUDIT_BEFORE.\nUse the moment this node was upgraded past the \
                 SSE replication defect — only versions written before it can be damaged, and \
                 without the bound the report counts healthy encrypted replicas too."
            );
            return ExitCode::FAILURE;
        }
    };
    let created_before = match replication_audit::parse_cutoff(raw) {
        Ok(ts) => ts,
        Err(e) => {
            eprintln!("--before: {e}");
            return ExitCode::FAILURE;
        }
    };

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async {
        let (meta, _oracle) = match stack::open_meta_store(&cfg).await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };

        // `--verify` is the only arm that needs bytes, keys and the network; the default audit is
        // pure metadata and opens neither the blob store nor the master ring.
        let verifier = if verify {
            match build_replica_verifier(&cfg, meta.clone()).await {
                Ok(v) => Some(v),
                Err(e) => {
                    eprintln!("failed to prepare --verify: {e}");
                    return ExitCode::FAILURE;
                }
            }
        } else {
            None
        };

        let report = match replication_audit::audit_store(
            meta.as_ref(),
            bucket,
            created_before,
            AUDIT_SAMPLE_LIMIT,
            cfg.replication_allow_plaintext_sse_over_http,
            cfg.replication_endpoint.as_deref(),
            verifier
                .as_ref()
                .map(|v| v.as_ref() as &dyn replication_audit::ReplicaVerifier),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                eprintln!("replication audit failed: {e}");
                return ExitCode::FAILURE;
            }
        };

        if json {
            match serde_json::to_string_pretty(&report) {
                Ok(s) => println!("{s}"),
                Err(e) => {
                    eprintln!("failed to render the audit as JSON: {e}");
                    return ExitCode::FAILURE;
                }
            }
            return ExitCode::SUCCESS;
        }

        if report.buckets.is_empty() {
            println!(
                "no bucket has an enabled replication rule; nothing to audit{}",
                bucket.map_or(String::new(), |b| format!(" (filtered to {b})"))
            );
            return ExitCode::SUCCESS;
        }
        println!(
            "replication audit (versions created before {}): {} present-and-suspect (ON the \
             mirror, possibly GARBAGE), {} absent (never landed), {} repair-pending (re-shipping \
             now), {} of the suspects NOT CURRENT (unrepairable here)",
            created_before.0,
            report.present_and_suspect,
            report.absent,
            report.repair_pending,
            report.non_current_suspect
        );
        // The done-state, stated so it is actually reachable. A forced requeue only moves a version
        // to `pending` when something can really ship it — it is current (the resync backfill
        // re-enqueues current versions) or it still has an outbox row — so repair-pending genuinely
        // drains to 0. The suspect count does NOT drain to 0 when non-current suspects exist: the
        // backfill cannot reach them (TRAP 2), so they stay, and that is the floor.
        if report.non_current_suspect > 0 {
            println!(
                "DONE = repair-pending 0 AND present-and-suspect {} (the non-current floor: those \
                 versions are unrepairable without rebuilding the destination bucket — see TRAP 2 \
                 below). A forced requeue moves rows into repair-pending, so suspects drop before \
                 any byte re-ships.",
                report.non_current_suspect
            );
        } else {
            println!(
                "the repair is complete when present-and-suspect AND repair-pending are BOTH 0 — a \
                 forced requeue moves rows into repair-pending, so suspects hit 0 before any byte \
                 re-ships."
            );
        }
        for b in &report.buckets {
            println!(
                "\nbucket {}: scanned={} present_and_suspect={} absent={} repair_pending={} \
                 non_current_suspect={} client_encrypted={}",
                b.bucket,
                b.versions_scanned,
                b.present_and_suspect,
                b.absent,
                b.repair_pending,
                b.non_current_suspect,
                b.client_encrypted_suspect
            );
            if verify {
                println!(
                    "  verified: matched={} MISMATCHED={} absent={} errors={} \
                     skipped_non_current={}",
                    b.verified_matched,
                    b.verified_mismatched,
                    b.verified_absent,
                    b.verify_errors,
                    b.verify_skipped_non_current
                );
                if b.verify_skipped_non_current > 0 {
                    println!(
                        "  (the byte check GETs the destination's CURRENT object — it carries no \
                         versionId — so comparing a superseded source version would report a false \
                         MISMATCH. Those {} are skipped, not verified clean.)",
                        b.verify_skipped_non_current
                    );
                }
            }
            // The three traps, printed where the operator is already looking (§C of the runbook in
            // docs/operations.md 8.7). Each of these silently wastes a repair pass.
            if b.present_and_suspect > 0 && !b.existing_object_replication {
                println!(
                    "  TRAP 1: no enabled rule sets ExistingObjectReplication — a resync will \
                     return success and repair NOTHING. Edit the rule first."
                );
            }
            if b.non_current_suspect > 0 {
                println!(
                    "  TRAP 2: {} suspect version(s) are NOT current. A resync backfill enumerates \
                     CURRENT versions only, so these are NOT repaired by any command here; full \
                     version-history fidelity requires rebuilding the destination bucket.",
                    b.non_current_suspect
                );
            }
            if b.repair_blocked_by_http_gate {
                println!(
                    "  TRAP 3: destination endpoint(s) {} are http:// and this bucket has \
                     client-encrypted suspects. Repair re-ships PLAINTEXT, so the confidentiality \
                     gate will refuse every one of them (rescheduled forever, never failed). Move \
                     the endpoint to https://, or set \
                     CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP=true before repairing.",
                    b.plaintext_http_endpoints.join(", ")
                );
            }
            for s in &b.samples {
                println!(
                    "  {} {} v={} size={} mode={}{}",
                    s.status,
                    s.key,
                    s.version_id,
                    s.size,
                    s.mode,
                    if s.is_latest { "" } else { " (non-current)" }
                );
            }
            let listed = b.samples.len() as u64;
            let total = b.present_and_suspect + b.absent;
            if total > listed {
                println!(
                    "  … and {} more (use --json for the full set)",
                    total - listed
                );
            }
        }
        if report.present_and_suspect > 0 {
            println!(
                "\nremediation: docs/operations.md 8.7. Repair re-ships UNCONDITIONALLY — never \
                 diff, the bytes are the wrong bytes at exactly the right length."
            );
        }
        ExitCode::SUCCESS
    })
}

/// Build the `--verify` byte checker: the local blob store + master ring to re-derive each source
/// version's **plaintext** MD5, and a per-bucket destination sink to fetch the replica.
async fn build_replica_verifier(
    cfg: &Config,
    meta: Arc<dyn cairn_types::traits::MetadataStore>,
) -> Result<Box<HttpReplicaVerifier>, String> {
    let blob = cairn_blob::LocalBlobStore::open(cfg.data_dir.clone())
        .await
        .map_err(|e| format!("opening the blob store: {e}"))?;
    let crypto = Arc::new(stack::build_crypto(cfg)?);
    Ok(Box::new(HttpReplicaVerifier {
        meta,
        blob,
        crypto,
        allow_internal_endpoints: cfg.allow_internal_endpoints,
        allow_plaintext_sse_over_http: cfg.replication_allow_plaintext_sse_over_http,
        sinks: tokio::sync::Mutex::new(std::collections::HashMap::new()),
    }))
}

/// The `--verify` implementation: GET the replica, compare it to the source plaintext MD5.
struct HttpReplicaVerifier {
    meta: Arc<dyn cairn_types::traits::MetadataStore>,
    blob: cairn_blob::LocalBlobStore,
    crypto: Arc<cairn_crypto::SystemCrypto>,
    allow_internal_endpoints: bool,
    allow_plaintext_sse_over_http: bool,
    /// Lazily-built destination sink per source bucket (`None` = this bucket has no resolvable
    /// target, so nothing can be verified for it).
    sinks: tokio::sync::Mutex<
        std::collections::HashMap<String, Option<Arc<cairn_replication::HttpS3Sink>>>,
    >,
}

impl HttpReplicaVerifier {
    /// Resolve (and memoize) the destination sink for a source bucket.
    async fn sink_for(
        &self,
        bucket: &cairn_types::id::BucketName,
    ) -> Option<Arc<cairn_replication::HttpS3Sink>> {
        let mut cache = self.sinks.lock().await;
        if let Some(hit) = cache.get(bucket.as_str()) {
            return hit.clone();
        }
        let built = self.build_sink(bucket).await;
        cache.insert(bucket.as_str().to_owned(), built.clone());
        built
    }

    async fn build_sink(
        &self,
        bucket: &cairn_types::id::BucketName,
    ) -> Option<Arc<cairn_replication::HttpS3Sink>> {
        use cairn_types::bucket::ConfigAspect;
        let rules = self
            .meta
            .get_bucket_config(bucket, ConfigAspect::Replication)
            .await
            .ok()??;
        let cfg = cairn_replication::parse_replication(rules.0.as_bytes()).ok()?;
        let arn = cfg
            .rules
            .iter()
            .find(|r| r.enabled)
            .and_then(|r| r.target_arn.clone())?;
        let targets_doc = self
            .meta
            .get_bucket_config(bucket, ConfigAspect::ReplicationTargets)
            .await
            .ok()??;
        let targets = cairn_replication::parse_targets(targets_doc.0.as_bytes()).ok()?;
        let target = cairn_replication::resolve_target(&targets, &arn)?;
        let open = cairn_replication::open_target(&self.crypto, target).ok()?;
        cairn_replication::sink_for_target(
            &open,
            self.allow_internal_endpoints,
            self.allow_plaintext_sse_over_http,
        )
        .ok()
        .map(Arc::new)
    }

    /// The source version's **plaintext** MD5, read through its own DEK. Never the stored ETag: a
    /// multipart-completed object's ETag is the composite `<md5>-<n>`, which is not the MD5 of
    /// anything the destination holds.
    async fn source_plaintext_md5(
        &self,
        row: &cairn_types::object::ObjectVersionRow,
    ) -> Result<String, String> {
        use cairn_types::traits::BlobStore;
        use futures_util::StreamExt;
        use md5::Digest;

        let path = row
            .storage_path
            .as_ref()
            .ok_or_else(|| "version has no backing blob".to_owned())?;
        let dek = match row.sse_descriptor.as_deref() {
            Some(json) => {
                let d = cairn_types::sse::parse_descriptor(json)
                    .map_err(|e| format!("parsing the sse descriptor: {e}"))?;
                Some(
                    *cairn_types::sse::open_dek(self.crypto.as_ref(), &d)
                        .map_err(|e| format!("unsealing the data key: {e}"))?,
                )
            }
            None => None,
        };
        let handle = self
            .blob
            .open_raw(
                path,
                None,
                cairn_types::blob::BlobCipher::from_dek(dek),
                &row.compression,
            )
            .await
            .map_err(|e| format!("reading the source blob: {e}"))?;
        let mut hasher = md5::Md5::new();
        let mut body = handle.body;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| format!("streaming the source blob: {e}"))?;
            hasher.update(&chunk);
        }
        Ok(hex::encode(hasher.finalize()))
    }
}

#[async_trait::async_trait]
impl replication_audit::ReplicaVerifier for HttpReplicaVerifier {
    async fn verify(
        &self,
        row: &cairn_types::object::ObjectVersionRow,
    ) -> replication_audit::VerifyOutcome {
        use cairn_types::error::ReplicationError;
        use md5::Digest;
        use replication_audit::VerifyOutcome;

        let Some(sink) = self.sink_for(&row.bucket).await else {
            return VerifyOutcome::Errored;
        };
        let want = match self.source_plaintext_md5(row).await {
            Ok(md5) => md5,
            Err(e) => {
                eprintln!(
                    "  verify {}/{}: cannot read the source plaintext: {e}",
                    row.bucket.as_str(),
                    row.key.as_str()
                );
                return VerifyOutcome::Errored;
            }
        };
        // The replica is hashed AS IT ARRIVES and never held: both sides of this comparison are
        // now O(1) in memory (`source_plaintext_md5` already streamed its blob through the same
        // kind of loop). The cap below bounds the bytes read, not a buffer.
        let mut hasher = md5::Md5::new();
        let streamed = sink
            .stream_object(
                row.bucket.as_str(),
                row.key.as_str(),
                AUDIT_VERIFY_MAX_BYTES,
                &mut |chunk: &[u8]| hasher.update(chunk),
            )
            .await;
        match streamed {
            Ok(_) => {
                let got = hex::encode(hasher.finalize());
                if got == want {
                    VerifyOutcome::Matched
                } else {
                    VerifyOutcome::Mismatched
                }
            }
            // A structural 404 is the `failed`/BadDigest population: the replica never landed.
            // This deliberately does NOT sniff the message for "404" — the terminal message quotes
            // the destination's response body, so any 4xx whose XML happens to carry those digits
            // (a request id, a key name, a size) would be reported as a benign absent replica.
            Err(ReplicationError::NotFound(_)) => VerifyOutcome::Absent,
            Err(_) => VerifyOutcome::Errored,
        }
    }
}

/// Open the metadata store (which runs any pending migrations) and report the resulting schema
/// version. The server runs the same migrations at startup; this command is for operators who
/// prefer to migrate explicitly (ARCH 11.2, 24.2).
fn migrate(cfg: Config) -> ExitCode {
    if let Some(parent) = cfg.db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    // `open` runs migrations on the write connection before returning (ARCH 11.2). We then read
    // the applied version directly from `schema_migrations` rather than holding the store, which
    // keeps this command a thin reporter over the migration the open already performed.
    match cairn_meta::open(&cfg.db_path, &cairn_meta::OpenOptions::default()) {
        Ok(_store) => {}
        Err(e) => {
            eprintln!("failed to open metadata store: {e}");
            return ExitCode::FAILURE;
        }
    }
    match schema_version(&cfg.db_path) {
        Ok(v) => {
            println!("migrations applied; schema version {v}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("failed to read schema version: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Read the highest applied migration version from the database file.
fn schema_version(db_path: &std::path::Path) -> Result<i64, String> {
    let conn = rusqlite::Connection::open(db_path).map_err(|e| e.to_string())?;
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |r| r.get::<_, i64>(0),
    )
    .map_err(|e| e.to_string())
}

/// Take a consistent snapshot into `dir` (ARCH 31.4). The database is snapshotted first
/// (checkpoint to fold the WAL into the main file, then copy it), and the blob tree is copied
/// second excluding the staging area. Taking the database first guarantees the copied blob set is
/// a superset of what the snapshot references, so restore finds a blob for every row; any extra
/// blobs from writes after the snapshot are reclaimed by reconciliation on restore. The master
/// key is deliberately not part of the data dir, so it is not disclosed by the snapshot.
fn backup(cfg: Config, dir: &std::path::Path) -> ExitCode {
    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        if let Err(e) = tokio::fs::create_dir_all(dir).await {
            eprintln!("failed to create backup dir: {e}");
            return ExitCode::FAILURE;
        }

        // 1. Database first: open (runs migrations), checkpoint to fold the WAL into the main
        //    file, then copy the now-self-contained database file.
        let store = match cairn_meta::open(&cfg.db_path, &cairn_meta::OpenOptions::default()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(e) = store.checkpoint().await {
            eprintln!("failed to checkpoint before snapshot: {e}");
            return ExitCode::FAILURE;
        }
        let db_name = match cfg.db_path.file_name() {
            Some(n) => n.to_owned(),
            None => {
                eprintln!("db_path has no file name: {}", cfg.db_path.display());
                return ExitCode::FAILURE;
            }
        };
        let db_dest = dir.join(&db_name);
        if let Err(e) = tokio::fs::copy(&cfg.db_path, &db_dest).await {
            eprintln!("failed to copy database: {e}");
            return ExitCode::FAILURE;
        }
        // Drop the store so its connections (and any -wal/-shm) are released before we finish.
        drop(store);

        // 2. Blobs second: copy every per-bucket directory, excluding the staging area.
        let blob_dest = dir.join("blobs");
        match copy_blob_tree(&cfg.data_dir, &blob_dest).await {
            Ok(n) => {
                println!(
                    "backup complete: database -> {} ({n} blob entries) -> {}",
                    db_dest.display(),
                    blob_dest.display()
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("failed to copy blob tree: {e}");
                ExitCode::FAILURE
            }
        }
    })
}

/// Restore a snapshot from `dir` into the configured data dir, then reconcile (ARCH 31.4). The
/// database and blob tree produced by `backup` are placed, and reconciliation reclaims any blobs
/// written after the snapshot was taken.
fn restore(cfg: Config, dir: &std::path::Path) -> ExitCode {
    use cairn_types::blob::ReconcileOpts;
    use cairn_types::traits::BlobStore;

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        let db_name = match cfg.db_path.file_name() {
            Some(n) => n.to_owned(),
            None => {
                eprintln!("db_path has no file name: {}", cfg.db_path.display());
                return ExitCode::FAILURE;
            }
        };
        let db_src = dir.join(&db_name);
        let blob_src = dir.join("blobs");
        if !db_src.exists() {
            eprintln!("snapshot is missing the database: {}", db_src.display());
            return ExitCode::FAILURE;
        }

        // 1. Place files: the blob tree into the data dir, then the database.
        if let Some(parent) = cfg.db_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        if let Err(e) = tokio::fs::create_dir_all(&cfg.data_dir).await {
            eprintln!("failed to create data dir: {e}");
            return ExitCode::FAILURE;
        }
        if blob_src.exists() {
            if let Err(e) = copy_blob_tree(&blob_src, &cfg.data_dir).await {
                eprintln!("failed to restore blob tree: {e}");
                return ExitCode::FAILURE;
            }
        }
        if let Err(e) = tokio::fs::copy(&db_src, &cfg.db_path).await {
            eprintln!("failed to restore database: {e}");
            return ExitCode::FAILURE;
        }

        // 2. Reconcile: reclaim any blobs from writes after the snapshot (ARCH 31.4).
        let store = match cairn_meta::open(&cfg.db_path, &cairn_meta::OpenOptions::default()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to open restored metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        let oracle = store.reconcile_oracle();
        let blob = match cairn_blob::LocalBlobStore::open(cfg.data_dir.clone()).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("failed to open blob store: {e}");
                return ExitCode::FAILURE;
            }
        };
        match blob
            .reconcile(
                &oracle,
                ReconcileOpts {
                    staging_safety_margin_secs: 0,
                    ..ReconcileOpts::default()
                },
            )
            .await
        {
            Ok(r) => {
                println!(
                    "restore complete: reconciled scanned={} orphans_reclaimed={}",
                    r.blobs_scanned, r.orphans_reclaimed
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("restore placed files but reconciliation failed: {e}");
                ExitCode::FAILURE
            }
        }
    })
}

/// Recursively copy the per-bucket blob directories from `src` to `dst`, skipping the `.staging`
/// area (in-progress writes are not part of a consistent snapshot, ARCH 31.4) and any database
/// sidecar files. Returns the number of top-level entries copied.
async fn copy_blob_tree(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<u64> {
    tokio::fs::create_dir_all(dst).await?;
    let mut copied = 0u64;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        // Exclude the staging area and the database files; only committed per-bucket blob
        // directories belong in the snapshot.
        if name_str == ".staging"
            || name_str.ends_with(".db")
            || name_str.ends_with(".db-wal")
            || name_str.ends_with(".db-shm")
        {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        if entry.file_type().await?.is_dir() {
            Box::pin(copy_dir_recursive(&from, &to)).await?;
        } else {
            tokio::fs::copy(&from, &to).await?;
        }
        copied += 1;
    }
    Ok(copied)
}

/// Recursively copy a directory and its contents.
async fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(dst).await?;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(entry) = entries.next_entry().await? {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type().await?.is_dir() {
            Box::pin(copy_dir_recursive(&from, &to)).await?;
        } else {
            tokio::fs::copy(&from, &to).await?;
        }
    }
    Ok(())
}

fn runtime(cfg: &Config) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // Size the blocking pool to cover the metadata read pool + blob I/O concurrency so neither
    // starves the other (ARCH 30); compute parallelism is pinned only when set explicitly.
    builder.max_blocking_threads(cfg.effective_max_blocking_threads());
    if let Some(workers) = cfg.effective_worker_threads() {
        builder.worker_threads(workers);
    }
    builder.build()
}

fn run_server(cfg: Config) -> ExitCode {
    observability::init_tracing(&cfg.log_level, cfg.log_format);
    let metrics = observability::init_metrics();

    // Arm the fault-injection registry from $FAILPOINTS (only in `failpoints` builds, used by the
    // crash-consistency harness). The scenario must outlive the server, so it is held here.
    #[cfg(feature = "failpoints")]
    let _fail_scenario = fail::FailScenario::setup();

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(async {
        let stack = match stack::build(&cfg).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                tracing::error!(error = %e, "failed to build engine stack");
                return ExitCode::FAILURE;
            }
        };
        match server::serve(cfg, metrics, stack).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(error = %e, "server exited with error");
                ExitCode::FAILURE
            }
        }
    })
}

fn bootstrap(cfg: Config) -> ExitCode {
    use cairn_types::traits::{Clock, Crypto};
    use std::sync::Arc;

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(async {
        if let Some(parent) = cfg.db_path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let _ = tokio::fs::create_dir_all(&cfg.data_dir).await;

        // Open through the configured backend (CAIRN_META_BACKEND) so the first administrator is
        // written into the same engine the server will later serve from.
        let store = match stack::open_meta_store(&cfg).await {
            Ok((meta, _oracle)) => meta,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        let crypto: Arc<dyn Crypto> = match stack::build_crypto(&cfg) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };
        let clock: Arc<dyn Clock> = Arc::new(cairn_crypto::SystemClock::new());

        // Seed exactly one default administrator — the root identity (CAIRN_ROOT_ACCESS_KEY /
        // CAIRN_ROOT_SECRET_KEY) that `serve` also ensures on every startup. Bootstrapping the SAME
        // identity (rather than minting a separate random "administrator") means `bootstrap` + `serve`
        // converge on a single "root" admin instead of leaving the node with two default admins.
        // Idempotent: re-running just re-affirms the root admin.
        if let Err(e) = stack::ensure_root_admin(&store, &crypto, &clock, &cfg).await {
            eprintln!("failed to seed the root administrator: {e}");
            return ExitCode::FAILURE;
        }

        let insecure_defaults =
            cfg.root_access_key == "cairn" && cfg.root_secret_key == "cairnadmin";
        // Print both credential forms with their canonical labels so tooling and the conformance
        // harnesses parse them: the Bearer token off the "Authorization: Bearer" line, and the SigV4
        // pair off the "Access Key Id:" / "Secret Access Key:" lines (last field).
        println!("Root administrator ready — the single default admin for this node.\n");
        println!("  Bearer (web console + management API):");
        println!(
            "    Authorization: Bearer {}.{}",
            cfg.root_access_key, cfg.root_secret_key
        );
        println!("\n  SigV4 (S3 SDKs / aws-cli):");
        println!("    Access Key Id:     {}", cfg.root_access_key);
        println!("    Secret Access Key: {}", cfg.root_secret_key);
        println!("    Region:            {}", cfg.region);
        println!("\n  Create further users from the console or `cairn remote user create`.",);
        if insecure_defaults {
            println!(
                "\n  WARNING: these are the INSECURE defaults (cairn / cairnadmin). Set\n  \
                 CAIRN_ROOT_ACCESS_KEY and CAIRN_ROOT_SECRET_KEY before exposing this node."
            );
        }
        ExitCode::SUCCESS
    })
}

#[cfg(test)]
mod tests {
    use super::{Cli, copy_blob_tree, schema_version};
    use clap::Parser;

    /// `--all-versions` widens a forced requeue, and does NOTHING without `--force`. It used to be
    /// accepted and silently ignored, so `cairn replication resync b --all-versions` reported
    /// success while doing the narrow thing — the exact "a repair that repaired nothing" failure
    /// this command exists to prevent. Clap must reject it instead.
    #[test]
    fn resync_all_versions_requires_force() {
        let err = Cli::try_parse_from(["cairn", "replication", "resync", "b", "--all-versions"])
            .expect_err("--all-versions without --force must be rejected, not silently ignored");
        let msg = err.to_string();
        assert!(
            msg.contains("--force"),
            "the error must point at the missing flag, got {msg:?}"
        );
        // With --force it parses, and either flag alone on the force path is fine.
        Cli::try_parse_from([
            "cairn",
            "replication",
            "resync",
            "b",
            "--force",
            "--all-versions",
        ])
        .expect("--force --all-versions is the widened repair");
        Cli::try_parse_from(["cairn", "replication", "resync", "b", "--force"])
            .expect("--force alone is the default encrypted-only repair");
    }

    /// The audit's cutoff is a real CLI argument in both accepted forms; the command must not
    /// quietly acquire a default, because the cutoff determines what every count in the report
    /// means.
    #[test]
    fn audit_accepts_a_before_cutoff() {
        for v in ["2026-07-23T10:00:00Z", "1753264800"] {
            Cli::try_parse_from(["cairn", "replication", "audit", "--before", v])
                .unwrap_or_else(|e| panic!("--before {v} must parse: {e}"));
        }
    }

    /// `copy_blob_tree` copies committed per-bucket blob directories but skips the staging area
    /// and database sidecars, so a snapshot contains only durable blobs (ARCH 31.4).
    #[tokio::test]
    async fn backup_copies_blobs_but_excludes_staging_and_db() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let root = src.path();

        // A committed blob under a per-bucket directory, plus a staging artifact and a db file.
        tokio::fs::create_dir_all(root.join("bucket-a"))
            .await
            .unwrap();
        tokio::fs::write(root.join("bucket-a").join("blob1"), b"committed")
            .await
            .unwrap();
        tokio::fs::create_dir_all(root.join(".staging"))
            .await
            .unwrap();
        tokio::fs::write(root.join(".staging").join("inflight.tmp"), b"partial")
            .await
            .unwrap();
        tokio::fs::write(root.join("cairn.db"), b"db")
            .await
            .unwrap();
        tokio::fs::write(root.join("cairn.db-wal"), b"wal")
            .await
            .unwrap();

        let copied = copy_blob_tree(root, dst.path()).await.unwrap();

        assert!(dst.path().join("bucket-a").join("blob1").exists());
        assert_eq!(
            tokio::fs::read(dst.path().join("bucket-a").join("blob1"))
                .await
                .unwrap(),
            b"committed"
        );
        assert!(!dst.path().join(".staging").exists(), "staging excluded");
        assert!(!dst.path().join("cairn.db").exists(), "db excluded");
        assert!(!dst.path().join("cairn.db-wal").exists(), "wal excluded");
        assert_eq!(copied, 1, "only the one bucket directory is copied");
    }

    /// A backup of the blob tree, restored into a fresh data dir, reproduces every committed blob
    /// and nothing from the staging area (the core of the 31.4 round-trip).
    #[tokio::test]
    async fn backup_restore_blob_tree_round_trips() {
        let src = tempfile::tempdir().unwrap();
        let snap = tempfile::tempdir().unwrap();
        let restored = tempfile::tempdir().unwrap();

        tokio::fs::create_dir_all(src.path().join("b1"))
            .await
            .unwrap();
        tokio::fs::write(src.path().join("b1").join("x"), b"one")
            .await
            .unwrap();
        tokio::fs::create_dir_all(src.path().join("b2/sub"))
            .await
            .unwrap();
        tokio::fs::write(src.path().join("b2/sub/y"), b"two")
            .await
            .unwrap();
        tokio::fs::create_dir_all(src.path().join(".staging"))
            .await
            .unwrap();
        tokio::fs::write(src.path().join(".staging/tmp"), b"junk")
            .await
            .unwrap();

        copy_blob_tree(src.path(), snap.path()).await.unwrap();
        copy_blob_tree(snap.path(), restored.path()).await.unwrap();

        assert_eq!(
            tokio::fs::read(restored.path().join("b1/x")).await.unwrap(),
            b"one"
        );
        assert_eq!(
            tokio::fs::read(restored.path().join("b2/sub/y"))
                .await
                .unwrap(),
            b"two"
        );
        assert!(!restored.path().join(".staging").exists());
    }

    /// Opening the store runs migrations; the schema version is then a positive integer.
    #[test]
    fn migrate_reports_positive_schema_version() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("cairn.db");
        let _store = cairn_meta::open(&db, &cairn_meta::OpenOptions::default()).unwrap();
        let v = schema_version(&db).unwrap();
        assert!(v >= 1, "migrations should have advanced the schema version");
    }
}
