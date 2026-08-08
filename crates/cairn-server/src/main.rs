//! The Cairn server binary entrypoint. Parses configuration, initialises observability, builds
//! the engine stack, and runs the HTTP server with ordered graceful shutdown. Also carries the
//! node-local commands that operate directly on the data dir from config: `bootstrap` (mint the
//! first administrator), `integrity` (on-demand reconciliation), `migrate` (run migrations and
//! report the schema version), and `backup`/`restore` (the ARCH 31.4 offline single-SQLite snapshot
//! and its inverse). The full remote-admin CLI ships as `cairn-cli` in a later wave.

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
mod error_page;
mod import_dest;
mod import_run;
mod key_rewrap;
mod metrics_agg;
mod multipart_claim_recovery;
mod node_lock;
mod observability;
mod proxy;
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
    /// Take an offline, validated single-SQLite snapshot into an empty DIR (ARCH 31.4).
    Backup {
        /// Destination directory for the snapshot (created if absent).
        dir: PathBuf,
    },
    /// Restore an offline, validated single-SQLite snapshot, then reconcile (ARCH 31.4).
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

    if matches!(&command, Command::Backup { .. } | Command::Restore { .. })
        && let Err(error) = require_canonical_backup_topology(&cfg)
    {
        eprintln!("{error}");
        return ExitCode::from(2);
    }

    // Every command that directly accesses node-local state cooperates on the data-root and
    // database locks. In particular this makes backup/restore explicitly offline relative to a
    // running Cairn process. `validate-config` remains side-effect-free.
    let _node_lock = if matches!(&command, Command::ValidateConfig) {
        None
    } else {
        match node_lock::NodeLock::acquire(&cfg.data_dir, &cfg.db_path) {
            Ok(lock) => Some(lock),
            Err(error) => {
                eprintln!(
                    "cannot access node-local state exclusively: {error}; stop the running Cairn \
                     server or other node-local command and retry"
                );
                return ExitCode::FAILURE;
            }
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
                Ok(report) if report.protected == 0 => {
                    println!(
                        "repair complete: dangling_rows_dropped={} protected_unresolved=0",
                        report.dropped
                    );
                    ExitCode::SUCCESS
                }
                Ok(report) => {
                    eprintln!(
                        "repair incomplete: dangling_rows_dropped={} protected_unresolved={}; \
                         Object Lock protected metadata was preserved",
                        report.dropped, report.protected
                    );
                    ExitCode::FAILURE
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
/// blob is absent. Protected rows remain as explicit unresolved damage rather than silently
/// erasing WORM metadata.
///
/// This composes only the public store/blob primitives (no privileged internals): it is the
/// node-local inverse of orphan reclamation and is deliberately destructive, so it runs only under
/// the explicit `--repair` flag.
async fn repair_dangling_rows(
    meta: &dyn cairn_types::traits::MetadataStore,
    blob: &cairn_blob::LocalBlobStore,
) -> Result<RepairDanglingReport, String> {
    use cairn_types::Clock;
    use cairn_types::error::BlobError;
    use cairn_types::meta::{ListQuery, Mutation, MutationOutcome};
    use cairn_types::object::GovernanceBypass;
    use cairn_types::traits::BlobStore;

    let buckets = meta.list_buckets(None).await.map_err(|e| e.to_string())?;
    let mut dropped = 0u64;
    let mut protected = 0u64;
    let now = cairn_crypto::SystemClock::new().now();

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
                                expected_row_id: None,
                                expected_updated_at: None,
                                require_sole_key_version: false,
                                now,
                                bypass: GovernanceBypass::Denied,
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
                            Ok(MutationOutcome::DeleteNotApplied) => {}
                            Ok(MutationOutcome::DeleteProtected) => protected += 1,
                            Ok(_) => {
                                return Err("unexpected integrity-repair delete outcome".to_owned());
                            }
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

    Ok(RepairDanglingReport { dropped, protected })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RepairDanglingReport {
    dropped: u64,
    protected: u64,
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
        sink_runtime: cfg.replication_sink_runtime(),
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
    sink_runtime: cairn_replication::ReplicationSinkRuntime,
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
            self.sink_runtime.clone(),
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
        let cipher = match row.sse_descriptor.as_deref() {
            Some(json) => {
                let d = cairn_types::sse::parse_descriptor(json)
                    .map_err(|e| format!("parsing the sse descriptor: {e}"))?;
                cairn_types::sse::open_blob_cipher(self.crypto.as_ref(), &d)
                    .map_err(|e| format!("unsealing the data key: {e}"))?
            }
            None => cairn_types::blob::BlobCipher::KnownPlaintext,
        };
        let handle = self
            .blob
            .open_raw(path, None, cipher, &row.compression, row.size_logical)
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
    ensure_regular_file_no_symlink(db_path, "SQLite database")?;
    let conn = rusqlite::Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| e.to_string())?;
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |r| r.get::<_, i64>(0),
    )
    .map_err(|e| e.to_string())
}

/// The backup/restore format currently has one deliberately narrow topology contract.
///
/// Alternate engines and sharded SQLite use multiple physical metadata stores. Treating any one of
/// them as a complete snapshot would let restore reclaim valid blobs as orphans, so refuse them
/// until a topology-aware manifest exists.
fn require_canonical_backup_topology(cfg: &Config) -> Result<(), String> {
    if cfg.meta_backend != "sqlite" || cfg.meta_shards != 1 {
        return Err(format!(
            "backup/restore supports only CAIRN_META_BACKEND=sqlite with \
             CAIRN_META_SHARDS=1; configured backend={:?}, shards={}",
            cfg.meta_backend, cfg.meta_shards
        ));
    }
    Ok(())
}

const SNAPSHOT_FORMAT_VERSION: u32 = 1;
const SNAPSHOT_BLOB_LAYOUT_VERSION: u32 = 1;
const SNAPSHOT_DATABASE_FILE: &str = "metadata.sqlite3";
const SNAPSHOT_BLOB_DIRECTORY: &str = "blobs";
const SNAPSHOT_MANIFEST_FILE: &str = "manifest.json";
const MAX_SNAPSHOT_MANIFEST_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotManifest {
    format_version: u32,
    complete: bool,
    created_at_unix_ms: u64,
    created_by: String,
    metadata: SnapshotMetadataManifest,
    blobs: SnapshotBlobManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotMetadataManifest {
    backend: String,
    shards: u32,
    schema_version: i64,
    database_file: String,
    database_size: u64,
    database_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotBlobManifest {
    layout_version: u32,
}

#[derive(Debug)]
struct ValidatedSnapshot {
    manifest: SnapshotManifest,
    database: std::path::PathBuf,
    blobs: std::path::PathBuf,
    referenced_files: u64,
}

/// Create a transactionally-consistent, self-contained SQLite file.
///
/// The process-wide [`node_lock::NodeLock`] already excludes every Cairn writer and blob mutator.
/// We still refuse a contended truncating checkpoint (which reveals an external WAL-pinning
/// reader/writer), then use SQLite's own `VACUUM INTO` snapshot facility rather than reading pages
/// with a filesystem copy.
async fn snapshot_sqlite_database(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> Result<(), String> {
    if destination.exists() {
        return Err(format!(
            "snapshot database destination already exists: {}",
            destination.display()
        ));
    }
    // Opening the concrete store may apply pending migrations, and every such write plus both
    // maintenance operations stay serialized on its one Writer.
    let store = cairn_meta::open(source, &cairn_meta::OpenOptions::default())
        .map_err(|error| format!("failed to open source SQLite metadata store: {error}"))?;
    let stats = store
        .checkpoint()
        .await
        .map_err(|error| format!("failed to checkpoint source SQLite database: {error}"))?;
    if stats.busy {
        return Err(format!(
            "source SQLite WAL is busy ({}/{} frames checkpointed); stop every reader/writer and \
             retry the offline backup",
            stats.checkpointed_frames, stats.log_frames
        ));
    }

    store
        .vacuum_into_snapshot(destination.to_owned())
        .await
        .map_err(|error| format!("SQLite snapshot failed: {error}"))?;
    let snapshot_file = tokio::fs::OpenOptions::new()
        .read(true)
        .open(destination)
        .await
        .map_err(|error| format!("failed to reopen snapshot database for sync: {error}"))?;
    snapshot_file
        .sync_all()
        .await
        .map_err(|error| format!("failed to sync snapshot database: {error}"))?;
    Ok(())
}

fn validate_snapshot_database(path: &std::path::Path) -> Result<(), String> {
    use rusqlite::{OpenFlags, OptionalExtension};

    ensure_regular_file_no_symlink(path, "snapshot database")?;
    let conn = rusqlite::Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("failed to open snapshot database: {error}"))?;
    let mut statement = conn
        .prepare("PRAGMA integrity_check")
        .map_err(|error| format!("failed to start SQLite integrity_check: {error}"))?;
    let mut rows = statement
        .query([])
        .map_err(|error| format!("failed to run SQLite integrity_check: {error}"))?;
    let first = rows
        .next()
        .map_err(|error| format!("failed to read SQLite integrity_check: {error}"))?
        .ok_or_else(|| "SQLite integrity_check returned no result".to_owned())?
        .get::<_, String>(0)
        .map_err(|error| format!("invalid SQLite integrity_check result: {error}"))?;
    let extra = rows
        .next()
        .map_err(|error| format!("failed to read SQLite integrity_check: {error}"))?
        .is_some();
    if first != "ok" || extra {
        return Err(format!("SQLite integrity_check failed: {first}"));
    }
    drop(rows);
    drop(statement);
    let foreign_key_violation: Option<(String, Option<i64>)> = conn
        .query_row("PRAGMA foreign_key_check", [], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .optional()
        .map_err(|error| format!("failed to run SQLite foreign_key_check: {error}"))?;
    if let Some((table, rowid)) = foreign_key_violation {
        return Err(format!(
            "SQLite foreign_key_check failed at table {table:?}, rowid {rowid:?}"
        ));
    }
    Ok(())
}

/// Verify that every durable object and multipart-part path named by the snapshot exists in its
/// copied blob tree. This is intentionally streaming: backup validation remains bounded by one
/// SQLite row and one filesystem stat regardless of object count.
fn verify_snapshot_blob_references(
    database: &std::path::Path,
    blob_root: &std::path::Path,
) -> Result<u64, String> {
    use rusqlite::OpenFlags;
    use std::path::Component;

    let conn = rusqlite::Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("failed to open snapshot database: {error}"))?;
    let mut statement = conn
        .prepare(
            "SELECT storage_path FROM object_versions WHERE storage_path IS NOT NULL
             UNION ALL
             SELECT storage_path FROM multipart_parts",
        )
        .map_err(|error| format!("failed to enumerate snapshot blob references: {error}"))?;
    let paths = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| format!("failed to enumerate snapshot blob references: {error}"))?;

    let mut checked = 0u64;
    for path in paths {
        let path = path.map_err(|error| format!("invalid snapshot blob reference: {error}"))?;
        let relative = std::path::Path::new(&path);
        if relative.as_os_str().is_empty()
            || relative.is_absolute()
            || !relative
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(format!(
                "unsafe storage_path in snapshot database: {path:?}"
            ));
        }
        let resolved = blob_root.join(relative);
        let metadata = std::fs::symlink_metadata(&resolved).map_err(|error| {
            format!(
                "snapshot is missing referenced blob {path:?} at {}: {error}",
                resolved.display()
            )
        })?;
        if !metadata.is_file() {
            return Err(format!(
                "referenced blob {path:?} is not a regular file at {}",
                resolved.display()
            ));
        }
        checked = checked.saturating_add(1);
    }
    Ok(checked)
}

fn ensure_regular_file_no_symlink(
    path: &std::path::Path,
    description: &str,
) -> Result<std::fs::Metadata, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("{description} {} is unavailable: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "{description} must be a non-symlink regular file: {}",
            path.display()
        ));
    }
    Ok(metadata)
}

fn ensure_directory_no_symlink(
    path: &std::path::Path,
    description: &str,
) -> Result<std::fs::Metadata, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("{description} {} is unavailable: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "{description} must be a non-symlink directory: {}",
            path.display()
        ));
    }
    Ok(metadata)
}

fn present_sqlite_sidecars(path: &std::path::Path) -> Result<Vec<std::path::PathBuf>, String> {
    let mut present = Vec::new();
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sqlite_sidecar_path(path, suffix);
        match std::fs::symlink_metadata(&sidecar) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(format!(
                        "SQLite sidecar must be a non-symlink regular file: {}",
                        sidecar.display()
                    ));
                }
                present.push(sidecar);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to inspect SQLite sidecar {}: {error}",
                    sidecar.display()
                ));
            }
        }
    }
    Ok(present)
}

async fn file_fingerprint(path: &std::path::Path) -> Result<(u64, String), String> {
    use sha2::Digest;
    use tokio::io::AsyncReadExt;

    ensure_regular_file_no_symlink(path, "snapshot file")?;
    let file = cairn_blob::open_readonly_nofollow(path).map_err(|error| {
        format!(
            "failed to open {} without following symlinks: {error}",
            path.display()
        )
    })?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "failed to stat opened snapshot file {}: {error}",
            path.display()
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "opened snapshot path is not a regular file: {}",
            path.display()
        ));
    }
    let expected_size = metadata.len();
    let mut file = tokio::fs::File::from_std(file);
    let mut hash = sha2::Sha256::new();
    let mut size = 0u64;
    let mut buffer = vec![0u8; 128 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|error| format!("failed to hash snapshot file {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        size = size.saturating_add(read as u64);
        hash.update(&buffer[..read]);
    }
    if size != expected_size {
        return Err(format!(
            "snapshot file changed while hashing {} (stat size {expected_size}, read {size})",
            path.display()
        ));
    }
    Ok((size, hex::encode(hash.finalize())))
}

async fn build_snapshot_manifest(database: &std::path::Path) -> Result<SnapshotManifest, String> {
    let (database_size, database_sha256) = file_fingerprint(database).await?;
    let schema_version = schema_version(database)?;
    let created_at_unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("system clock is before the Unix epoch: {error}"))?
        .as_millis()
        .try_into()
        .map_err(|_| "snapshot creation timestamp does not fit in u64".to_owned())?;
    Ok(SnapshotManifest {
        format_version: SNAPSHOT_FORMAT_VERSION,
        complete: true,
        created_at_unix_ms,
        created_by: format!("cairn/{}", CAIRN_VERSION.trim()),
        metadata: SnapshotMetadataManifest {
            backend: "sqlite".to_owned(),
            shards: 1,
            schema_version,
            database_file: SNAPSHOT_DATABASE_FILE.to_owned(),
            database_size,
            database_sha256,
        },
        blobs: SnapshotBlobManifest {
            layout_version: SNAPSHOT_BLOB_LAYOUT_VERSION,
        },
    })
}

async fn write_snapshot_manifest_last(
    snapshot_root: &std::path::Path,
    manifest: &SnapshotManifest,
) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;

    let destination = snapshot_root.join(SNAPSHOT_MANIFEST_FILE);
    if destination.exists() {
        return Err(format!(
            "snapshot completion manifest already exists: {}",
            destination.display()
        ));
    }
    let staged = snapshot_root.join(format!(
        ".{SNAPSHOT_MANIFEST_FILE}.{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));
    let mut bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| format!("failed to serialize snapshot manifest: {error}"))?;
    bytes.push(b'\n');
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staged)
            .await
            .map_err(|error| format!("failed to create staged snapshot manifest: {error}"))?;
        file.write_all(&bytes)
            .await
            .map_err(|error| format!("failed to write staged snapshot manifest: {error}"))?;
        file.sync_all()
            .await
            .map_err(|error| format!("failed to sync staged snapshot manifest: {error}"))?;
        drop(file);
        tokio::fs::rename(&staged, &destination)
            .await
            .map_err(|error| format!("failed to publish snapshot completion manifest: {error}"))?;
        sync_directory(snapshot_root)
            .await
            .map_err(|error| format!("failed to sync completed snapshot directory: {error}"))?;
        if let Some(parent) = snapshot_root.parent()
            && !parent.as_os_str().is_empty()
        {
            sync_directory(parent)
                .await
                .map_err(|error| format!("failed to sync snapshot parent directory: {error}"))?;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = remove_file_if_present(&staged).await;
    }
    result
}

async fn read_snapshot_manifest(
    snapshot_root: &std::path::Path,
) -> Result<SnapshotManifest, String> {
    use tokio::io::AsyncReadExt;

    ensure_directory_no_symlink(snapshot_root, "snapshot root")?;
    let path = snapshot_root.join(SNAPSHOT_MANIFEST_FILE);
    let metadata = ensure_regular_file_no_symlink(&path, "snapshot completion manifest")?;
    if metadata.len() > MAX_SNAPSHOT_MANIFEST_BYTES {
        return Err(format!(
            "snapshot manifest exceeds {} bytes: {}",
            MAX_SNAPSHOT_MANIFEST_BYTES,
            path.display()
        ));
    }
    let file = cairn_blob::open_readonly_nofollow(&path)
        .map_err(|error| format!("failed to open snapshot manifest safely: {error}"))?;
    let mut file = tokio::fs::File::from_std(file).take(MAX_SNAPSHOT_MANIFEST_BYTES + 1);
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)
        .await
        .map_err(|error| format!("failed to read snapshot manifest: {error}"))?;
    if bytes.len() as u64 > MAX_SNAPSHOT_MANIFEST_BYTES {
        return Err(
            "snapshot manifest changed while reading and exceeded its size limit".to_owned(),
        );
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        format!("snapshot manifest is invalid JSON or has unknown fields: {error}")
    })
}

fn validate_snapshot_manifest(manifest: &SnapshotManifest) -> Result<(), String> {
    if manifest.format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(format!(
            "unsupported snapshot format version {} (this binary supports {})",
            manifest.format_version, SNAPSHOT_FORMAT_VERSION
        ));
    }
    if !manifest.complete {
        return Err("snapshot manifest is not marked complete".to_owned());
    }
    if manifest.created_at_unix_ms == 0 || manifest.created_by.trim().is_empty() {
        return Err("snapshot manifest has invalid creation metadata".to_owned());
    }
    if manifest.metadata.backend != "sqlite" || manifest.metadata.shards != 1 {
        return Err(format!(
            "unsupported snapshot metadata topology: backend={:?}, shards={}",
            manifest.metadata.backend, manifest.metadata.shards
        ));
    }
    if manifest.metadata.database_file != SNAPSHOT_DATABASE_FILE {
        return Err(format!(
            "snapshot database_file must be the fixed name {SNAPSHOT_DATABASE_FILE:?}, got {:?}",
            manifest.metadata.database_file
        ));
    }
    let latest = cairn_meta::latest_schema_version();
    if manifest.metadata.schema_version <= 0 {
        return Err(format!(
            "snapshot schema version must be positive, got {}",
            manifest.metadata.schema_version
        ));
    }
    if manifest.metadata.schema_version > latest {
        return Err(format!(
            "snapshot schema version {} is newer than this binary understands ({latest})",
            manifest.metadata.schema_version
        ));
    }
    if manifest.metadata.database_sha256.len() != 64
        || !manifest
            .metadata
            .database_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(
            "snapshot database SHA-256 must be 64 lowercase hexadecimal characters".to_owned(),
        );
    }
    if manifest.blobs.layout_version != SNAPSHOT_BLOB_LAYOUT_VERSION {
        return Err(format!(
            "unsupported blob-layout version {} (this binary supports {})",
            manifest.blobs.layout_version, SNAPSHOT_BLOB_LAYOUT_VERSION
        ));
    }
    Ok(())
}

async fn validate_snapshot(snapshot_root: &std::path::Path) -> Result<ValidatedSnapshot, String> {
    let manifest = read_snapshot_manifest(snapshot_root).await?;
    validate_snapshot_manifest(&manifest)?;
    let database = snapshot_root.join(SNAPSHOT_DATABASE_FILE);
    let blobs = snapshot_root.join(SNAPSHOT_BLOB_DIRECTORY);
    ensure_regular_file_no_symlink(&database, "snapshot database")?;
    ensure_directory_no_symlink(&blobs, "snapshot blob root")?;

    let sidecars = present_sqlite_sidecars(&database)?;
    if !sidecars.is_empty() {
        return Err(format!(
            "snapshot contains forbidden SQLite sidecar {}; the database image must be \
             self-contained",
            sidecars[0].display()
        ));
    }

    let (size, sha256) = file_fingerprint(&database).await?;
    if size != manifest.metadata.database_size {
        return Err(format!(
            "snapshot database size mismatch: manifest={}, actual={size}",
            manifest.metadata.database_size
        ));
    }
    if sha256 != manifest.metadata.database_sha256 {
        return Err(format!(
            "snapshot database SHA-256 mismatch: manifest={}, actual={sha256}",
            manifest.metadata.database_sha256
        ));
    }
    validate_snapshot_database(&database)?;
    let actual_schema = schema_version(&database)?;
    if actual_schema != manifest.metadata.schema_version {
        return Err(format!(
            "snapshot schema-version mismatch: manifest={}, database={actual_schema}",
            manifest.metadata.schema_version
        ));
    }
    validate_blob_tree_layout(&blobs).await?;
    let referenced_files = verify_snapshot_blob_references(&database, &blobs)?;
    Ok(ValidatedSnapshot {
        manifest,
        database,
        blobs,
        referenced_files,
    })
}

fn ensure_empty_snapshot_destination(dir: &std::path::Path) -> Result<(), String> {
    match std::fs::symlink_metadata(dir) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(format!(
                    "backup destination must be a non-symlink directory: {}",
                    dir.display()
                ));
            }
            let mut entries = std::fs::read_dir(dir)
                .map_err(|error| format!("failed to inspect backup destination: {error}"))?;
            if entries
                .next()
                .transpose()
                .map_err(|error| format!("failed to inspect backup destination: {error}"))?
                .is_some()
            {
                return Err(format!(
                    "backup destination must be empty: {}",
                    dir.display()
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(dir)
                .map_err(|error| format!("failed to create backup destination: {error}"))?;
            ensure_directory_no_symlink(dir, "backup destination")?;
        }
        Err(error) => {
            return Err(format!(
                "failed to inspect backup destination {}: {error}",
                dir.display()
            ));
        }
    }
    Ok(())
}

fn reject_overlapping_trees(
    first: &std::path::Path,
    second: &std::path::Path,
    operation: &str,
) -> Result<(), String> {
    let first = std::fs::canonicalize(first)
        .map_err(|error| format!("failed to resolve {}: {error}", first.display()))?;
    let second = std::fs::canonicalize(second)
        .map_err(|error| format!("failed to resolve {}: {error}", second.display()))?;
    if first.starts_with(&second) || second.starts_with(&first) {
        return Err(format!(
            "{operation} source and destination trees overlap: {} and {}",
            first.display(),
            second.display()
        ));
    }
    Ok(())
}

/// Take an explicitly offline, internally-validated snapshot into `dir` (ARCH 31.4).
fn backup(cfg: Config, dir: &std::path::Path) -> ExitCode {
    if let Err(error) = require_canonical_backup_topology(&cfg) {
        eprintln!("{error}");
        return ExitCode::from(2);
    }
    if let Err(error) = ensure_empty_snapshot_destination(dir) {
        eprintln!("{error}");
        return ExitCode::FAILURE;
    }
    if let Err(error) = reject_overlapping_trees(&cfg.data_dir, dir, "backup") {
        eprintln!("{error}");
        return ExitCode::FAILURE;
    }

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        let db_dest = dir.join(SNAPSHOT_DATABASE_FILE);
        if let Err(error) = snapshot_sqlite_database(&cfg.db_path, &db_dest).await {
            eprintln!("failed to snapshot database: {error}");
            return ExitCode::FAILURE;
        }
        if let Err(error) = validate_snapshot_database(&db_dest) {
            eprintln!("snapshot database validation failed: {error}");
            return ExitCode::FAILURE;
        }

        // The node lock makes the source tree quiescent. Copy committed object blobs plus durable
        // multipart parts; transient single-object `.staging/*.tmp` files are never metadata-
        // referenced and remain excluded.
        let blob_dest = dir.join(SNAPSHOT_BLOB_DIRECTORY);
        let excluded = match database_artifact_names(&cfg.data_dir, &cfg.db_path) {
            Ok(names) => names,
            Err(error) => {
                eprintln!("failed to identify database artifacts: {error}");
                return ExitCode::FAILURE;
            }
        };
        match copy_blob_tree(&cfg.data_dir, &blob_dest, &excluded).await {
            Ok(n) => {
                let referenced = match verify_snapshot_blob_references(&db_dest, &blob_dest) {
                    Ok(referenced) => referenced,
                    Err(error) => {
                        eprintln!("snapshot blob validation failed: {error}");
                        return ExitCode::FAILURE;
                    }
                };
                if let Err(error) = sync_directory(dir).await {
                    eprintln!("failed to sync snapshot contents before completion: {error}");
                    return ExitCode::FAILURE;
                }
                let manifest = match build_snapshot_manifest(&db_dest).await {
                    Ok(manifest) => manifest,
                    Err(error) => {
                        eprintln!("failed to build snapshot manifest: {error}");
                        return ExitCode::FAILURE;
                    }
                };
                if let Err(error) = write_snapshot_manifest_last(dir, &manifest).await {
                    eprintln!("failed to complete snapshot manifest: {error}");
                    return ExitCode::FAILURE;
                }
                println!(
                    "backup complete: offline single-SQLite snapshot database={} \
                     manifest={} ({n} blob entries, {referenced} referenced files verified) \
                     blobs={}",
                    db_dest.display(),
                    dir.join(SNAPSHOT_MANIFEST_FILE).display(),
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

/// Restore an offline single-SQLite snapshot, validating it before replacing any metadata.
fn restore(cfg: Config, dir: &std::path::Path) -> ExitCode {
    use cairn_types::blob::ReconcileOpts;
    use cairn_types::traits::BlobStore;

    if let Err(error) = require_canonical_backup_topology(&cfg) {
        eprintln!("{error}");
        return ExitCode::from(2);
    }
    if !dir.is_dir() {
        eprintln!("snapshot directory does not exist: {}", dir.display());
        return ExitCode::FAILURE;
    }
    if let Err(error) = reject_overlapping_trees(&cfg.data_dir, dir, "restore") {
        eprintln!("{error}");
        return ExitCode::FAILURE;
    }

    let rt = match runtime(&cfg) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        // The manifest is the completion marker. Validate its topology/schema ceiling, creation
        // metadata, exact DB size+digest, sidecar-free SQLite image, referenced files, and complete
        // non-symlink blob-tree shape before creating or modifying any target-owned path.
        let snapshot = match validate_snapshot(dir).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                eprintln!("snapshot validation failed: {error}");
                return ExitCode::FAILURE;
            }
        };

        // Copy into a target-owned sibling and validate that inode against the manifest. A source
        // swap or short copy therefore cannot reach the publication rename.
        let staged =
            match stage_snapshot_database(&snapshot.database, &cfg.db_path, &snapshot.manifest)
                .await
            {
                Ok(staged) => staged,
                Err(error) => {
                    eprintln!("failed to stage restored database: {error}");
                    return ExitCode::FAILURE;
                }
            };

        // Make any old sidecar-owning generation self-contained before the publication name can
        // change. Crashing before rename then reopens old state; crashing after rename sees only
        // the new, independently validated image.
        if let Err(error) = prepare_target_database_for_publish(&cfg.db_path).await {
            eprintln!("failed to prepare current database generation: {error}");
            return ExitCode::FAILURE;
        }

        // Blob-first durability: if copying stops before metadata publication, the old database
        // remains authoritative and any newly introduced opaque files are only orphans.
        if let Err(error) = copy_blob_tree(&snapshot.blobs, &cfg.data_dir, &[]).await {
            eprintln!("failed to restore blob tree: {error}");
            return ExitCode::FAILURE;
        }

        // Revalidate the target-owned staging inode immediately before the one metadata
        // linearization point. Publication also refuses if an old sidecar has reappeared.
        if let Err(error) = publish_staged_database(staged, &cfg.db_path, &snapshot.manifest).await
        {
            eprintln!("failed to restore database: {error}");
            return ExitCode::FAILURE;
        }
        if let Err(error) =
            validate_database_against_manifest(&cfg.db_path, &snapshot.manifest).await
        {
            eprintln!("restored database validation failed: {error}");
            return ExitCode::FAILURE;
        }

        // Reconcile while the node lock is still held, before any listener can serve the restored
        // state. This reclaims target-side blobs that were not present in the snapshot.
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
                if let Err(error) = validate_restore_reconcile_report(&r) {
                    eprintln!("restore placed files but reconciliation was incomplete: {error}");
                    return ExitCode::FAILURE;
                }
                println!(
                    "restore complete: reconciled scanned={} orphans_reclaimed={}; offline \
                     single-SQLite snapshot verified {} referenced files",
                    r.blobs_scanned, r.orphans_reclaimed, snapshot.referenced_files
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

fn validate_restore_reconcile_report(
    report: &cairn_types::blob::ReconcileReport,
) -> Result<(), String> {
    if report.errors == 0 {
        Ok(())
    } else {
        Err(format!(
            "reconciliation reported {} non-fatal error(s) after scanning {} blob(s)",
            report.errors, report.blobs_scanned
        ))
    }
}

fn database_artifact_names(
    data_root: &std::path::Path,
    db_path: &std::path::Path,
) -> std::io::Result<Vec<std::ffi::OsString>> {
    let data_root = std::fs::canonicalize(data_root)?;
    let db_parent = db_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let db_parent = std::fs::canonicalize(db_parent)?;
    if data_root != db_parent {
        return Ok(Vec::new());
    }
    let name = db_path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("database path has no file name: {}", db_path.display()),
        )
    })?;
    let mut wal = name.to_os_string();
    wal.push("-wal");
    let mut shm = name.to_os_string();
    shm.push("-shm");
    let mut journal = name.to_os_string();
    journal.push("-journal");
    Ok(vec![name.to_os_string(), wal, shm, journal])
}

fn sqlite_sidecar_path(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    std::path::PathBuf::from(value)
}

async fn remove_file_if_present(path: &std::path::Path) -> std::io::Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[derive(Debug)]
struct StagedDatabase {
    path: std::path::PathBuf,
    published: bool,
}

impl StagedDatabase {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for StagedDatabase {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn validate_target_database_entry(destination: &std::path::Path) -> Result<(), String> {
    match std::fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(format!(
            "target database must be a non-symlink regular file when it exists: {}",
            destination.display()
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "failed to inspect target database {}: {error}",
            destination.display()
        )),
    }
}

async fn validate_database_against_manifest(
    database: &std::path::Path,
    manifest: &SnapshotManifest,
) -> Result<(), String> {
    let sidecars = present_sqlite_sidecars(database)?;
    if !sidecars.is_empty() {
        return Err(format!(
            "staged snapshot database unexpectedly has sidecar {}",
            sidecars[0].display()
        ));
    }
    let (size, sha256) = file_fingerprint(database).await?;
    if size != manifest.metadata.database_size || sha256 != manifest.metadata.database_sha256 {
        return Err(format!(
            "staged database digest mismatch: expected {} bytes/{}, got {size} bytes/{sha256}",
            manifest.metadata.database_size, manifest.metadata.database_sha256
        ));
    }
    validate_snapshot_database(database)?;
    let version = schema_version(database)?;
    if version != manifest.metadata.schema_version {
        return Err(format!(
            "staged database schema mismatch: expected {}, got {version}",
            manifest.metadata.schema_version
        ));
    }
    Ok(())
}

async fn stage_snapshot_database(
    source: &std::path::Path,
    destination: &std::path::Path,
    manifest: &SnapshotManifest,
) -> Result<StagedDatabase, String> {
    validate_target_database_entry(destination)?;
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| format!("failed to create database parent: {error}"))?;
    ensure_directory_no_symlink(parent, "target database parent")?;
    let name = destination
        .file_name()
        .ok_or_else(|| format!("database path has no file name: {}", destination.display()))?;
    let staged = parent.join(format!(
        ".{}.restore-{}.tmp",
        name.to_string_lossy(),
        uuid::Uuid::new_v4().simple()
    ));
    durable_copy_file(source, &staged)
        .await
        .map_err(|error| format!("failed to stage snapshot database: {error}"))?;
    let staged = StagedDatabase {
        path: staged,
        published: false,
    };
    validate_database_against_manifest(staged.path(), manifest).await?;
    Ok(staged)
}

async fn sync_regular_file_no_symlink(path: &std::path::Path) -> Result<(), String> {
    ensure_regular_file_no_symlink(path, "database")?;
    let file = cairn_blob::open_readonly_nofollow(path)
        .map_err(|error| format!("failed to open database safely for sync: {error}"))?;
    tokio::fs::File::from_std(file)
        .sync_all()
        .await
        .map_err(|error| format!("failed to sync database {}: {error}", path.display()))
}

/// Make the currently published database generation self-contained before it can be replaced.
///
/// If no exact SQLite sidecar exists, a corrupt main file remains replaceable. When a sidecar is
/// present, however, only SQLite may fold it into the main file: the canonical Writer runs a
/// truncating checkpoint, every owned connection closes, the main file is synced, and only then
/// are the exact sidecars removed and their parent directory synced.
async fn prepare_target_database_for_publish(destination: &std::path::Path) -> Result<(), String> {
    validate_target_database_entry(destination)?;
    let sidecars = present_sqlite_sidecars(destination)?;
    if sidecars.is_empty() {
        return Ok(());
    }
    ensure_regular_file_no_symlink(destination, "target database with SQLite sidecars")?;
    let store =
        cairn_meta::open(destination, &cairn_meta::OpenOptions::default()).map_err(|error| {
            format!("failed to open old database generation for consolidation: {error}")
        })?;
    let stats = store.checkpoint_and_close().await.map_err(|error| {
        format!("failed to checkpoint and close old database generation: {error}")
    })?;
    if stats.busy {
        return Err(format!(
            "old database WAL is busy ({}/{} frames checkpointed); target generation was not \
             replaced",
            stats.checkpointed_frames, stats.log_frames
        ));
    }
    sync_regular_file_no_symlink(destination).await?;
    for sidecar in present_sqlite_sidecars(destination)? {
        remove_file_if_present(&sidecar).await.map_err(|error| {
            format!(
                "failed to remove consolidated sidecar {}: {error}",
                sidecar.display()
            )
        })?;
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    sync_directory(parent)
        .await
        .map_err(|error| format!("failed to sync database parent after sidecar removal: {error}"))
}

async fn publish_staged_database(
    mut staged: StagedDatabase,
    destination: &std::path::Path,
    manifest: &SnapshotManifest,
) -> Result<(), String> {
    validate_database_against_manifest(staged.path(), manifest).await?;
    validate_target_database_entry(destination)?;
    let sidecars = present_sqlite_sidecars(destination)?;
    if !sidecars.is_empty() {
        return Err(format!(
            "refusing to publish while old-generation sidecar still exists: {}",
            sidecars[0].display()
        ));
    }
    tokio::fs::rename(staged.path(), destination)
        .await
        .map_err(|error| format!("failed to atomically publish restored database: {error}"))?;
    staged.published = true;
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    sync_directory(parent)
        .await
        .map_err(|error| format!("failed to sync published database generation: {error}"))
}

fn invalid_filesystem_entry(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn require_source_directory(path: &std::path::Path) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(invalid_filesystem_entry(format!(
            "source path must be a non-symlink directory: {}",
            path.display()
        )));
    }
    Ok(())
}

async fn ensure_safe_destination_directory(path: &std::path::Path) -> std::io::Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(invalid_filesystem_entry(format!(
                "destination path must be a non-symlink directory: {}",
                path.display()
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir(path).await
        }
        Err(error) => Err(error),
    }
}

async fn validate_blob_tree_layout(root: &std::path::Path) -> Result<(), String> {
    require_source_directory(root)
        .map_err(|error| format!("invalid snapshot blob root {}: {error}", root.display()))?;
    let mut entries = tokio::fs::read_dir(root)
        .await
        .map_err(|error| format!("failed to inspect snapshot blob root: {error}"))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| format!("failed to inspect snapshot blob root: {error}"))?
    {
        let file_type = entry
            .file_type()
            .await
            .map_err(|error| format!("failed to classify snapshot blob entry: {error}"))?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if file_type.is_symlink() {
            return Err(format!(
                "snapshot blob tree contains symlink: {}",
                entry.path().display()
            ));
        }
        if name_str == ".staging" {
            if !file_type.is_dir() {
                return Err(format!(
                    "snapshot .staging entry is not a directory: {}",
                    entry.path().display()
                ));
            }
            let mut staging_entries = tokio::fs::read_dir(entry.path()).await.map_err(|error| {
                format!("failed to inspect snapshot staging directory: {error}")
            })?;
            while let Some(staging_entry) = staging_entries
                .next_entry()
                .await
                .map_err(|error| format!("failed to inspect snapshot staging directory: {error}"))?
            {
                if staging_entry.file_name() != "multipart" {
                    return Err(format!(
                        "snapshot staging directory contains unexpected entry: {}",
                        staging_entry.path().display()
                    ));
                }
                let staging_type = staging_entry.file_type().await.map_err(|error| {
                    format!("failed to classify snapshot multipart directory: {error}")
                })?;
                if staging_type.is_symlink() || !staging_type.is_dir() {
                    return Err(format!(
                        "snapshot multipart staging path is not a non-symlink directory: {}",
                        staging_entry.path().display()
                    ));
                }
                Box::pin(validate_blob_directory_recursive(&staging_entry.path())).await?;
            }
            continue;
        }
        if !file_type.is_dir() {
            return Err(format!(
                "snapshot blob root contains a top-level non-directory entry: {}",
                entry.path().display()
            ));
        }
        Box::pin(validate_blob_directory_recursive(&entry.path())).await?;
    }
    Ok(())
}

async fn validate_blob_directory_recursive(path: &std::path::Path) -> Result<(), String> {
    require_source_directory(path).map_err(|error| {
        format!(
            "invalid snapshot blob directory {}: {error}",
            path.display()
        )
    })?;
    let mut entries = tokio::fs::read_dir(path)
        .await
        .map_err(|error| format!("failed to inspect snapshot blob directory: {error}"))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| format!("failed to inspect snapshot blob directory: {error}"))?
    {
        let file_type = entry
            .file_type()
            .await
            .map_err(|error| format!("failed to classify snapshot blob entry: {error}"))?;
        if file_type.is_symlink() {
            return Err(format!(
                "snapshot blob tree contains symlink: {}",
                entry.path().display()
            ));
        }
        if file_type.is_dir() {
            Box::pin(validate_blob_directory_recursive(&entry.path())).await?;
        } else if !file_type.is_file() {
            return Err(format!(
                "snapshot blob tree contains a non-regular entry: {}",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

/// Recursively copy committed object blobs and durable multipart parts. Transient
/// `.staging/*.tmp`, Cairn advisory locks, and the explicitly-named database artifacts are
/// excluded. Every copied path is a non-symlink directory or regular file, and a top-level regular
/// file is never interpreted as a blob bucket. Returns the number of top-level blob entries copied.
async fn copy_blob_tree(
    src: &std::path::Path,
    dst: &std::path::Path,
    excluded_database_names: &[std::ffi::OsString],
) -> std::io::Result<u64> {
    require_source_directory(src)?;
    ensure_safe_destination_directory(dst).await?;
    let mut copied = 0u64;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let file_type = entry.file_type().await?;
        if file_type.is_symlink() {
            return Err(invalid_filesystem_entry(format!(
                "refusing to copy symlink {}",
                entry.path().display()
            )));
        }
        if node_lock::is_lock_file_name(&name_str)
            || excluded_database_names
                .iter()
                .any(|excluded| excluded == &name)
        {
            if !file_type.is_file() {
                return Err(invalid_filesystem_entry(format!(
                    "excluded node artifact is not a regular file: {}",
                    entry.path().display()
                )));
            }
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        if name_str == ".staging" {
            if !file_type.is_dir() {
                return Err(invalid_filesystem_entry(format!(
                    "staging root is not a directory: {}",
                    from.display()
                )));
            }
            let multipart = from.join("multipart");
            match tokio::fs::symlink_metadata(&multipart).await {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                    return Err(invalid_filesystem_entry(format!(
                        "multipart staging path is not a non-symlink directory: {}",
                        multipart.display()
                    )));
                }
                Ok(_) => {
                    ensure_safe_destination_directory(&to).await?;
                    let staging_dest = to.join("multipart");
                    Box::pin(copy_dir_recursive(&multipart, &staging_dest)).await?;
                    sync_directory(&to).await?;
                    copied = copied.saturating_add(1);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            continue;
        }
        if !file_type.is_dir() {
            return Err(invalid_filesystem_entry(format!(
                "top-level blob entry must be a bucket directory, not a file or special node: {}",
                from.display()
            )));
        }
        Box::pin(copy_dir_recursive(&from, &to)).await?;
        copied = copied.saturating_add(1);
    }
    sync_directory(dst).await?;
    Ok(copied)
}

/// Recursively copy a directory and its contents.
async fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    require_source_directory(src)?;
    ensure_safe_destination_directory(dst).await?;
    let mut entries = tokio::fs::read_dir(src).await?;
    while let Some(entry) = entries.next_entry().await? {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let file_type = entry.file_type().await?;
        if file_type.is_symlink() {
            return Err(invalid_filesystem_entry(format!(
                "refusing to copy symlink {}",
                from.display()
            )));
        }
        if file_type.is_dir() {
            Box::pin(copy_dir_recursive(&from, &to)).await?;
        } else if file_type.is_file() {
            durable_copy_file(&from, &to).await?;
        } else {
            return Err(invalid_filesystem_entry(format!(
                "refusing to copy non-regular filesystem entry {}",
                from.display()
            )));
        }
    }
    sync_directory(dst).await
}

async fn durable_copy_file(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> std::io::Result<()> {
    let source_file = open_regular_file_nofollow(source, "copy source")?;
    let opened_metadata = source_file.metadata()?;
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    ensure_safe_destination_directory(parent).await?;

    match tokio::fs::symlink_metadata(destination).await {
        Ok(_) => {
            let destination_file = open_regular_file_nofollow(destination, "copy destination")?;
            if regular_files_are_byte_identical(source_file, destination_file).await? {
                // Immutable blob identities may collide across generations. Reuse the existing
                // inode only when its bytes are exact; replacing even an identical file would
                // create a crash window in which the old metadata generation loses its blob.
                return Ok(());
            }
            return Err(immutable_blob_collision(source, destination));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let staged = parent.join(format!(".cairn-copy-{}.tmp", uuid::Uuid::new_v4().simple()));
    let result = async {
        let mut input = tokio::fs::File::from_std(source_file);
        let mut output = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staged)
            .await?;
        let copied = tokio::io::copy(&mut input, &mut output).await?;
        if copied != opened_metadata.len() {
            return Err(invalid_filesystem_entry(format!(
                "source file changed size while copying {} (expected {}, copied {copied})",
                source.display(),
                opened_metadata.len()
            )));
        }
        output.sync_all().await?;
        drop(output);
        publish_staged_copy_no_replace(&staged, destination, parent).await
    }
    .await;
    if result.is_err() {
        let _ = remove_file_if_present(&staged).await;
    }
    result
}

fn open_regular_file_nofollow(
    path: &std::path::Path,
    role: &str,
) -> std::io::Result<std::fs::File> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(invalid_filesystem_entry(format!(
            "{role} must be a non-symlink regular file: {}",
            path.display()
        )));
    }
    let file = cairn_blob::open_readonly_nofollow(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid_filesystem_entry(format!(
            "opened {role} is not a regular file: {}",
            path.display()
        )));
    }
    Ok(file)
}

async fn regular_files_are_byte_identical(
    mut left: std::fs::File,
    mut right: std::fs::File,
) -> std::io::Result<bool> {
    let left_len = left.metadata()?.len();
    if right.metadata()?.len() != left_len {
        return Ok(false);
    }
    tokio::task::spawn_blocking(move || {
        use std::io::Read;

        const COMPARE_BUFFER_BYTES: usize = 64 * 1024;
        let mut left_buf = vec![0u8; COMPARE_BUFFER_BYTES];
        let mut right_buf = vec![0u8; COMPARE_BUFFER_BYTES];
        let mut remaining = left_len;
        while remaining != 0 {
            let chunk = usize::try_from(remaining.min(COMPARE_BUFFER_BYTES as u64))
                .expect("bounded comparison chunk fits usize");
            left.read_exact(&mut left_buf[..chunk])?;
            right.read_exact(&mut right_buf[..chunk])?;
            if left_buf[..chunk] != right_buf[..chunk] {
                return Ok(false);
            }
            remaining -= chunk as u64;
        }
        // Detect a concurrent append after the initial fstat instead of accepting a moving file.
        let mut extra = [0u8; 1];
        Ok(left.read(&mut extra)? == 0 && right.read(&mut extra)? == 0)
    })
    .await
    .map_err(|error| std::io::Error::other(format!("blob comparison task failed: {error}")))?
}

fn immutable_blob_collision(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!(
            "refusing to replace non-identical immutable blob {} with {}",
            destination.display(),
            source.display()
        ),
    )
}

async fn publish_staged_copy_no_replace(
    staged: &std::path::Path,
    destination: &std::path::Path,
    parent: &std::path::Path,
) -> std::io::Result<()> {
    match tokio::fs::hard_link(staged, destination).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            // An external filesystem actor may have created the name after our initial check.
            // Re-open both sides without following symlinks and apply the same immutable-collision
            // rule; never turn this race into a replacing rename.
            let staged_file = open_regular_file_nofollow(staged, "staged copy")?;
            let destination_file = open_regular_file_nofollow(destination, "copy destination")?;
            if !regular_files_are_byte_identical(staged_file, destination_file).await? {
                return Err(immutable_blob_collision(staged, destination));
            }
        }
        Err(error) => return Err(error),
    }
    remove_file_if_present(staged).await?;
    sync_directory(parent).await
}

async fn sync_directory(path: &std::path::Path) -> std::io::Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all())
        .await
        .map_err(|error| std::io::Error::other(format!("directory sync task failed: {error}")))?
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
            cfg.root_access_key,
            cfg.root_secret_key.expose_secret()
        );
        println!("\n  SigV4 (S3 SDKs / aws-cli):");
        println!("    Access Key Id:     {}", cfg.root_access_key);
        println!(
            "    Secret Access Key: {}",
            cfg.root_secret_key.expose_secret()
        );
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
    use super::{
        Cli, Config, SNAPSHOT_BLOB_DIRECTORY, SNAPSHOT_DATABASE_FILE, SNAPSHOT_MANIFEST_FILE,
        SnapshotManifest, build_snapshot_manifest, copy_blob_tree, database_artifact_names,
        prepare_target_database_for_publish, publish_staged_copy_no_replace,
        publish_staged_database, repair_dangling_rows, require_canonical_backup_topology,
        schema_version, snapshot_sqlite_database, stage_snapshot_database,
        validate_restore_reconcile_report, validate_snapshot, validate_snapshot_database,
        verify_snapshot_blob_references, write_snapshot_manifest_last,
    };
    use clap::Parser;

    async fn create_complete_empty_snapshot(
        workspace: &std::path::Path,
    ) -> (std::path::PathBuf, SnapshotManifest) {
        let snapshot = workspace.join("snapshot");
        std::fs::create_dir(&snapshot).unwrap();
        std::fs::create_dir(snapshot.join(SNAPSHOT_BLOB_DIRECTORY)).unwrap();
        let source = workspace.join("source.db");
        let database = snapshot.join(SNAPSHOT_DATABASE_FILE);
        snapshot_sqlite_database(&source, &database).await.unwrap();
        let manifest = build_snapshot_manifest(&database).await.unwrap();
        write_snapshot_manifest_last(&snapshot, &manifest)
            .await
            .unwrap();
        (snapshot, manifest)
    }

    fn dangling_row(bucket: &cairn_types::BucketName, key: &str) -> cairn_types::ObjectVersionRow {
        let now = cairn_types::Timestamp::from_secs(1);
        cairn_types::ObjectVersionRow {
            id: uuid::Uuid::new_v4().simple().to_string(),
            bucket: bucket.clone(),
            key: cairn_types::ObjectKey::parse(key).unwrap(),
            version_id: cairn_types::VersionId::generate(),
            is_latest: true,
            is_delete_marker: false,
            size_logical: 1,
            size_physical: 1,
            etag: cairn_types::ETag::from_md5_hex("9dd4e461268c8034f5c8564e155c67a6".to_owned()),
            content_type: "application/octet-stream".to_owned(),
            content_encoding: None,
            cache_control: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            storage_path: Some(cairn_types::StoragePath::generate(bucket)),
            compression: cairn_types::CompressionDescriptor::Uncompressed,
            storage_class: cairn_types::StorageClass::Standard,
            cold_locator: None,
            owner_id: cairn_types::UserId("admin".to_owned()),
            user_metadata: Vec::new(),
            acl: None,
            checksums: Vec::new(),
            sse_descriptor: None,
            replication_status: None,
            replicated_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn integrity_repair_preserves_protected_dangling_rows_and_reports_incomplete() {
        use cairn_types::traits::MetadataStore;

        let workspace = tempfile::tempdir().unwrap();
        let blob = cairn_blob::LocalBlobStore::open(workspace.path().join("blobs"))
            .await
            .unwrap();
        let meta = cairn_types::testing::InMemoryMetadataStore::new();
        let bucket_name = cairn_types::BucketName::parse("worm-repair").unwrap();
        let bucket = cairn_types::Bucket {
            name: bucket_name.clone(),
            owner_id: cairn_types::UserId("admin".to_owned()),
            created_at: cairn_types::Timestamp::from_secs(1),
            versioning: cairn_types::VersioningState::Enabled,
            ownership_mode: cairn_types::OwnershipMode::BucketOwnerEnforced,
            region: "us-east-1".to_owned(),
            compression: None,
        };
        meta.submit(cairn_types::Mutation::CreateObjectLockBucket(Box::new(
            bucket,
        )))
        .await
        .unwrap();

        let protected = dangling_row(&bucket_name, "protected");
        let protected_version = protected.version_id.clone();
        meta.submit(cairn_types::Mutation::PutObjectVersion {
            row: Box::new(protected),
            precondition: cairn_types::Precondition::default(),
            initial_state: cairn_types::InitialObjectState {
                tags: Vec::new(),
                lock_intent: cairn_types::ExplicitObjectLockIntent {
                    retention: Some(cairn_types::ObjectRetention {
                        mode: cairn_types::ObjectLockMode::Compliance,
                        retain_until: cairn_types::Timestamp(i64::MAX / 2),
                    }),
                    legal_hold: None,
                },
            },
            replication: Vec::new(),
        })
        .await
        .unwrap();

        let held = dangling_row(&bucket_name, "held");
        let held_version = held.version_id.clone();
        meta.submit(cairn_types::Mutation::PutObjectVersion {
            row: Box::new(held),
            precondition: cairn_types::Precondition::default(),
            initial_state: cairn_types::InitialObjectState {
                tags: Vec::new(),
                lock_intent: cairn_types::ExplicitObjectLockIntent {
                    retention: None,
                    legal_hold: Some(true),
                },
            },
            replication: Vec::new(),
        })
        .await
        .unwrap();

        let unprotected = dangling_row(&bucket_name, "unprotected");
        let unprotected_version = unprotected.version_id.clone();
        meta.submit(cairn_types::Mutation::PutObjectVersion {
            row: Box::new(unprotected),
            precondition: cairn_types::Precondition::default(),
            initial_state: cairn_types::InitialObjectState::default(),
            replication: Vec::new(),
        })
        .await
        .unwrap();

        let report = repair_dangling_rows(&meta, &blob).await.unwrap();
        assert_eq!(report.dropped, 1);
        assert_eq!(report.protected, 2);
        assert!(
            meta.get_version(
                &bucket_name,
                &cairn_types::ObjectKey::parse("protected").unwrap(),
                &protected_version,
            )
            .await
            .unwrap()
            .is_some(),
            "repair must preserve the WORM metadata even when its blob is missing"
        );
        assert!(
            meta.get_version(
                &bucket_name,
                &cairn_types::ObjectKey::parse("held").unwrap(),
                &held_version,
            )
            .await
            .unwrap()
            .is_some(),
            "repair must preserve a legally held version even when its blob is missing"
        );
        assert!(
            meta.get_version(
                &bucket_name,
                &cairn_types::ObjectKey::parse("unprotected").unwrap(),
                &unprotected_version,
            )
            .await
            .unwrap()
            .is_none(),
            "ordinary dangling metadata remains repairable"
        );
    }

    #[test]
    #[ignore = "subprocess helper for the WAL crash-boundary regression"]
    fn sqlite_wal_crash_helper() {
        let Ok(path) = std::env::var("CAIRN_TEST_WAL_CRASH_PATH") else {
            return;
        };
        let connection = rusqlite::Connection::open(path).unwrap();
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 CREATE TABLE generation (value TEXT NOT NULL);
                 INSERT INTO generation VALUES ('old-wal');",
            )
            .unwrap();
        // Model process death: `process::exit` deliberately skips Rust destructors, so SQLite does
        // not run its last-connection close checkpoint and the committed generation remains in WAL.
        std::process::exit(17);
    }

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

    #[test]
    fn backup_restore_rejects_every_noncanonical_metadata_topology() {
        let mut cfg = Config {
            meta_backend: "sqlite".to_owned(),
            meta_shards: 1,
            ..Config::default()
        };
        require_canonical_backup_topology(&cfg).unwrap();

        cfg.meta_shards = 2;
        assert!(
            require_canonical_backup_topology(&cfg)
                .unwrap_err()
                .contains("CAIRN_META_SHARDS=1")
        );
        cfg.meta_shards = 1;
        for backend in ["libsql", "turso"] {
            cfg.meta_backend = backend.to_owned();
            let error = require_canonical_backup_topology(&cfg).unwrap_err();
            assert!(error.contains("CAIRN_META_BACKEND=sqlite"), "{error}");
        }
    }

    #[tokio::test]
    async fn snapshot_manifest_is_completion_marker_and_binds_database_bytes_and_schema() {
        let workspace = tempfile::tempdir().unwrap();
        let (snapshot, manifest) = create_complete_empty_snapshot(workspace.path()).await;
        validate_snapshot(&snapshot)
            .await
            .expect("complete manifest and matching image validate");

        std::fs::remove_file(snapshot.join(SNAPSHOT_MANIFEST_FILE)).unwrap();
        let error = validate_snapshot(&snapshot).await.unwrap_err();
        assert!(error.contains("completion manifest"), "{error}");
        write_snapshot_manifest_last(&snapshot, &manifest)
            .await
            .unwrap();

        let database = snapshot.join(SNAPSHOT_DATABASE_FILE);
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&database)
            .unwrap()
            .write_all(b"tamper")
            .unwrap();
        let error = validate_snapshot(&snapshot).await.unwrap_err();
        assert!(
            error.contains("size mismatch") || error.contains("SHA-256 mismatch"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn snapshot_rejects_forward_schema_and_exact_sqlite_sidecars() {
        let workspace = tempfile::tempdir().unwrap();
        let (snapshot, mut manifest) = create_complete_empty_snapshot(workspace.path()).await;
        let manifest_path = snapshot.join(SNAPSHOT_MANIFEST_FILE);

        manifest.metadata.schema_version = cairn_meta::latest_schema_version() + 1;
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        let error = validate_snapshot(&snapshot).await.unwrap_err();
        assert!(error.contains("newer than this binary"), "{error}");

        manifest.metadata.schema_version = cairn_meta::latest_schema_version();
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        for suffix in ["-wal", "-shm", "-journal"] {
            let sidecar =
                super::sqlite_sidecar_path(&snapshot.join(SNAPSHOT_DATABASE_FILE), suffix);
            std::fs::write(&sidecar, b"forbidden").unwrap();
            let error = validate_snapshot(&snapshot).await.unwrap_err();
            assert!(error.contains("forbidden SQLite sidecar"), "{error}");
            std::fs::remove_file(sidecar).unwrap();
        }
        std::fs::write(
            snapshot.join(SNAPSHOT_BLOB_DIRECTORY).join("cairn.db"),
            b"must never overwrite target metadata",
        )
        .unwrap();
        let error = validate_snapshot(&snapshot).await.unwrap_err();
        assert!(error.contains("top-level non-directory"), "{error}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn snapshot_rejects_symlinked_manifest_database_and_blob_entries() {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir().unwrap();
        let (snapshot, manifest) = create_complete_empty_snapshot(workspace.path()).await;
        let manifest_path = snapshot.join(SNAPSHOT_MANIFEST_FILE);
        let real_manifest = snapshot.join("real-manifest.json");
        std::fs::rename(&manifest_path, &real_manifest).unwrap();
        symlink(&real_manifest, &manifest_path).unwrap();
        let error = validate_snapshot(&snapshot).await.unwrap_err();
        assert!(error.contains("non-symlink regular file"), "{error}");

        std::fs::remove_file(&manifest_path).unwrap();
        std::fs::rename(&real_manifest, &manifest_path).unwrap();
        let database = snapshot.join(SNAPSHOT_DATABASE_FILE);
        let real_database = snapshot.join("real-database.sqlite3");
        std::fs::rename(&database, &real_database).unwrap();
        symlink(&real_database, &database).unwrap();
        let error = validate_snapshot(&snapshot).await.unwrap_err();
        assert!(error.contains("non-symlink regular file"), "{error}");

        std::fs::remove_file(&database).unwrap();
        std::fs::rename(&real_database, &database).unwrap();
        let bucket = snapshot.join(SNAPSHOT_BLOB_DIRECTORY).join("bucket");
        std::fs::create_dir(&bucket).unwrap();
        symlink(&manifest_path, bucket.join("blob")).unwrap();
        let error = validate_snapshot(&snapshot).await.unwrap_err();
        assert!(error.contains("symlink"), "{error}");

        // Keep the compiler aware that the manifest remains the one whose digest matched the
        // restored regular database after the symlink checks.
        assert_eq!(manifest.metadata.database_file, SNAPSHOT_DATABASE_FILE);
    }

    /// `copy_blob_tree` preserves committed objects and durable multipart parts, but excludes
    /// transient staging files, advisory locks, and only the exact configured DB artifacts.
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
        tokio::fs::create_dir_all(root.join(".staging/multipart/upload-1"))
            .await
            .unwrap();
        tokio::fs::write(root.join(".staging").join("inflight.tmp"), b"partial")
            .await
            .unwrap();
        tokio::fs::write(
            root.join(".staging/multipart/upload-1/part-1"),
            b"durable part",
        )
        .await
        .unwrap();
        tokio::fs::write(root.join("cairn.db"), b"db")
            .await
            .unwrap();
        tokio::fs::write(root.join("cairn.db-wal"), b"wal")
            .await
            .unwrap();
        tokio::fs::write(root.join(".cairn-data.lock"), b"")
            .await
            .unwrap();
        // A bucket name ending in `.db` is not the configured database and must not be discarded
        // by a broad suffix filter.
        tokio::fs::create_dir_all(root.join("archive.db"))
            .await
            .unwrap();
        tokio::fs::write(root.join("archive.db/blob2"), b"bucket")
            .await
            .unwrap();

        let excluded = database_artifact_names(root, &root.join("cairn.db")).unwrap();
        let copied = copy_blob_tree(root, dst.path(), &excluded).await.unwrap();

        assert!(dst.path().join("bucket-a").join("blob1").exists());
        assert_eq!(
            tokio::fs::read(dst.path().join("bucket-a").join("blob1"))
                .await
                .unwrap(),
            b"committed"
        );
        assert!(
            !dst.path().join(".staging/inflight.tmp").exists(),
            "transient staging excluded"
        );
        assert_eq!(
            tokio::fs::read(dst.path().join(".staging/multipart/upload-1/part-1"))
                .await
                .unwrap(),
            b"durable part"
        );
        assert!(!dst.path().join("cairn.db").exists(), "db excluded");
        assert!(!dst.path().join("cairn.db-wal").exists(), "wal excluded");
        assert!(!dst.path().join(".cairn-data.lock").exists());
        assert_eq!(
            tokio::fs::read(dst.path().join("archive.db/blob2"))
                .await
                .unwrap(),
            b"bucket"
        );
        assert_eq!(copied, 3, "two buckets plus durable multipart staging");
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

        copy_blob_tree(src.path(), snap.path(), &[]).await.unwrap();
        copy_blob_tree(snap.path(), restored.path(), &[])
            .await
            .unwrap();

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

    /// Restore may encounter the same immutable storage path in the old and snapshot generations.
    /// Exact bytes reuse the old inode; different bytes fail without changing the old generation.
    #[cfg(unix)]
    #[tokio::test]
    async fn restore_blob_copy_reuses_only_byte_identical_collisions() {
        use std::os::unix::fs::MetadataExt;

        let source = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(source.path().join("bucket"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(destination.path().join("bucket"))
            .await
            .unwrap();
        let source_blob = source.path().join("bucket/blob");
        let destination_blob = destination.path().join("bucket/blob");
        tokio::fs::write(&source_blob, b"same immutable bytes")
            .await
            .unwrap();
        tokio::fs::write(&destination_blob, b"same immutable bytes")
            .await
            .unwrap();
        let original_inode = std::fs::metadata(&destination_blob).unwrap().ino();

        copy_blob_tree(source.path(), destination.path(), &[])
            .await
            .unwrap();
        assert_eq!(
            std::fs::metadata(&destination_blob).unwrap().ino(),
            original_inode,
            "an identical collision must reuse, not replace, the old generation's inode"
        );

        tokio::fs::write(&source_blob, b"different snapshot bytes")
            .await
            .unwrap();
        let error = copy_blob_tree(source.path(), destination.path(), &[])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(
            tokio::fs::read(&destination_blob).await.unwrap(),
            b"same immutable bytes",
            "a collision must leave the old generation byte-exact"
        );
        assert_eq!(
            std::fs::metadata(&destination_blob).unwrap().ino(),
            original_inode,
            "a differing collision must never replace the destination inode"
        );
    }

    /// Atomic no-replace publication re-checks an `AlreadyExists` race. An identical winner is
    /// accepted without replacement; a different winner remains untouched and fails.
    #[cfg(unix)]
    #[tokio::test]
    async fn staged_blob_publish_rechecks_an_already_exists_race() {
        use std::os::unix::fs::MetadataExt;

        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("blob");
        let different = directory.path().join(".cairn-copy-different.tmp");
        std::fs::write(&destination, b"winner").unwrap();
        std::fs::write(&different, b"loser").unwrap();
        let destination_inode = std::fs::metadata(&destination).unwrap().ino();

        let error = publish_staged_copy_no_replace(&different, &destination, directory.path())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&destination).unwrap(), b"winner");
        assert_eq!(
            std::fs::metadata(&destination).unwrap().ino(),
            destination_inode
        );

        let identical = directory.path().join(".cairn-copy-identical.tmp");
        std::fs::write(&identical, b"winner").unwrap();
        publish_staged_copy_no_replace(&identical, &destination, directory.path())
            .await
            .unwrap();
        assert!(!identical.exists(), "accepted staged alias is reclaimed");
        assert_eq!(
            std::fs::metadata(&destination).unwrap().ino(),
            destination_inode,
            "the raced destination is reused rather than replaced"
        );
    }

    #[test]
    fn restore_reconciliation_report_errors_are_fatal() {
        let clean = cairn_types::ReconcileReport {
            blobs_scanned: 7,
            ..Default::default()
        };
        assert!(validate_restore_reconcile_report(&clean).is_ok());

        let incomplete = cairn_types::ReconcileReport {
            blobs_scanned: 7,
            errors: 1,
            ..Default::default()
        };
        let error = validate_restore_reconcile_report(&incomplete).unwrap_err();
        assert!(error.contains("1 non-fatal error"), "{error}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn blob_copy_rejects_top_level_files_and_source_or_destination_symlinks() {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir().unwrap();
        let source = workspace.path().join("source");
        let destination = workspace.path().join("destination");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&destination).unwrap();

        std::fs::write(source.join("cairn.db"), b"must not become a blob").unwrap();
        let error = copy_blob_tree(&source, &destination, &[])
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("top-level blob entry"),
            "{error}"
        );
        assert!(
            !destination.join("cairn.db").exists(),
            "a malicious blobs/cairn.db must never overwrite metadata"
        );

        std::fs::remove_file(source.join("cairn.db")).unwrap();
        std::fs::create_dir(source.join("bucket")).unwrap();
        let outside_source = workspace.path().join("outside-source");
        std::fs::write(&outside_source, b"secret").unwrap();
        symlink(&outside_source, source.join("bucket/blob")).unwrap();
        let error = copy_blob_tree(&source, &destination, &[])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("symlink"), "{error}");

        std::fs::remove_file(source.join("bucket/blob")).unwrap();
        std::fs::write(source.join("bucket/blob"), b"snapshot").unwrap();
        std::fs::remove_dir(destination.join("bucket")).unwrap();
        let outside_destination = workspace.path().join("outside-destination");
        std::fs::create_dir(&outside_destination).unwrap();
        symlink(&outside_destination, destination.join("bucket")).unwrap();
        let error = copy_blob_tree(&source, &destination, &[])
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("destination path must be a non-symlink directory"),
            "{error}"
        );
        assert!(
            !outside_destination.join("blob").exists(),
            "an intermediate destination symlink must not escape the target tree"
        );
    }

    #[tokio::test]
    async fn sqlite_snapshot_refuses_a_live_writer_and_wal_pinning_reader() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.db");
        let destination = root.path().join("snapshot.db");
        let writer = rusqlite::Connection::open(&source).unwrap();
        writer.pragma_update(None, "journal_mode", "WAL").unwrap();
        writer
            .execute_batch(
                "CREATE TABLE values_for_backup (v INTEGER NOT NULL);
                 INSERT INTO values_for_backup VALUES (1);",
            )
            .unwrap();

        let reader = rusqlite::Connection::open(&source).unwrap();
        reader.execute_batch("BEGIN").unwrap();
        assert_eq!(
            reader
                .query_row("SELECT COUNT(*) FROM values_for_backup", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
        writer
            .execute("INSERT INTO values_for_backup VALUES (2)", [])
            .unwrap();
        writer
            .execute_batch(
                "BEGIN IMMEDIATE;
                 INSERT INTO values_for_backup VALUES (3);",
            )
            .unwrap();

        let error = snapshot_sqlite_database(&source, &destination)
            .await
            .expect_err("a live writer plus pinned WAL reader must be rejected");
        assert!(
            error.contains("checkpoint")
                || error.contains("busy")
                || error.contains("locked")
                || error.contains("open source"),
            "{error}"
        );
        assert!(!destination.exists());

        writer.execute_batch("ROLLBACK").unwrap();
        reader.execute_batch("COMMIT").unwrap();
        snapshot_sqlite_database(&source, &destination)
            .await
            .expect("quiescent SQLite snapshots through VACUUM INTO");
        validate_snapshot_database(&destination).unwrap();
        let snapshot = rusqlite::Connection::open(&destination).unwrap();
        assert_eq!(
            snapshot
                .query_row("SELECT COUNT(*) FROM values_for_backup", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            2,
            "committed rows are present and the rolled-back writer is absent"
        );
    }

    #[tokio::test]
    async fn snapshot_restore_round_trip_validates_every_referenced_file() {
        let root = tempfile::tempdir().unwrap();
        let source_db = root.path().join("source.db");
        let snapshot_db = root.path().join("snapshot.db");
        let restored_db = root.path().join("restored.db");
        let source_blobs = root.path().join("source-blobs");
        let snapshot_blobs = root.path().join("snapshot-blobs");
        std::fs::create_dir_all(source_blobs.join("bucket")).unwrap();
        std::fs::create_dir_all(source_blobs.join(".staging/multipart/upload")).unwrap();
        std::fs::write(source_blobs.join("bucket/object"), b"object").unwrap();
        std::fs::write(source_blobs.join(".staging/multipart/upload/part"), b"part").unwrap();

        let conn = rusqlite::Connection::open(&source_db).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_migrations (
                 version INTEGER PRIMARY KEY,
                 name TEXT NOT NULL,
                 applied_at INTEGER NOT NULL
             );
             INSERT INTO schema_migrations VALUES (1, 'test', 1);
             CREATE TABLE object_versions (storage_path TEXT);
             CREATE TABLE multipart_parts (storage_path TEXT NOT NULL);
             INSERT INTO object_versions VALUES ('bucket/object');
             INSERT INTO multipart_parts VALUES ('.staging/multipart/upload/part');",
        )
        .unwrap();
        conn.execute(
            "VACUUM INTO ?1",
            rusqlite::params![snapshot_db.to_str().unwrap()],
        )
        .unwrap();
        drop(conn);
        copy_blob_tree(&source_blobs, &snapshot_blobs, &[])
            .await
            .unwrap();
        assert_eq!(
            verify_snapshot_blob_references(&snapshot_db, &snapshot_blobs).unwrap(),
            2
        );
        let manifest = build_snapshot_manifest(&snapshot_db).await.unwrap();
        std::fs::write(&restored_db, b"old database generation").unwrap();
        let staged = stage_snapshot_database(&snapshot_db, &restored_db, &manifest)
            .await
            .unwrap();
        prepare_target_database_for_publish(&restored_db)
            .await
            .unwrap();
        publish_staged_database(staged, &restored_db, &manifest)
            .await
            .unwrap();
        assert!(!super::sqlite_sidecar_path(&restored_db, "-wal").exists());
        assert!(!super::sqlite_sidecar_path(&restored_db, "-shm").exists());
        validate_snapshot_database(&restored_db).unwrap();
        assert_eq!(
            verify_snapshot_blob_references(&restored_db, &snapshot_blobs).unwrap(),
            2
        );

        std::fs::remove_file(snapshot_blobs.join("bucket/object")).unwrap();
        let error = verify_snapshot_blob_references(&restored_db, &snapshot_blobs).unwrap_err();
        assert!(error.contains("missing referenced blob"), "{error}");
    }

    #[tokio::test]
    async fn database_publication_is_generation_safe_across_both_crash_boundaries() {
        let root = tempfile::tempdir().unwrap();
        let old_database = root.path().join("target.db");
        let new_source = root.path().join("new-source.db");
        let snapshot_database = root.path().join("new-snapshot.db");

        let old_store =
            cairn_meta::open(&old_database, &cairn_meta::OpenOptions::default()).unwrap();
        old_store.checkpoint_and_close().await.unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "tests::sqlite_wal_crash_helper",
                "--nocapture",
            ])
            .env("CAIRN_TEST_WAL_CRASH_PATH", &old_database)
            .status()
            .unwrap();
        assert_eq!(
            status.code(),
            Some(17),
            "WAL crash helper must take the exit path"
        );
        assert!(
            super::sqlite_sidecar_path(&old_database, "-wal").exists(),
            "crashed SQLite writer must leave a real old-generation WAL for this regression"
        );

        let new_store = cairn_meta::open(&new_source, &cairn_meta::OpenOptions::default()).unwrap();
        new_store.checkpoint_and_close().await.unwrap();
        let new_writer = rusqlite::Connection::open(&new_source).unwrap();
        new_writer
            .execute_batch(
                "CREATE TABLE generation (value TEXT NOT NULL);
                 INSERT INTO generation VALUES ('new-main');",
            )
            .unwrap();
        drop(new_writer);
        snapshot_sqlite_database(&new_source, &snapshot_database)
            .await
            .unwrap();
        let manifest = build_snapshot_manifest(&snapshot_database).await.unwrap();
        let staged = stage_snapshot_database(&snapshot_database, &old_database, &manifest)
            .await
            .unwrap();

        prepare_target_database_for_publish(&old_database)
            .await
            .unwrap();
        for suffix in ["-wal", "-shm", "-journal"] {
            assert!(
                !super::sqlite_sidecar_path(&old_database, suffix).exists(),
                "{suffix} must be absent and its directory synced before publication"
            );
        }

        // Crash immediately before rename: a copy of the published main file must reopen with the
        // complete old generation and no help from a sidecar.
        let crash_before = root.path().join("crash-before.db");
        std::fs::copy(&old_database, &crash_before).unwrap();
        let old_reopen = rusqlite::Connection::open(&crash_before).unwrap();
        assert_eq!(
            old_reopen
                .query_row("SELECT value FROM generation", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "old-wal"
        );
        drop(old_reopen);

        publish_staged_database(staged, &old_database, &manifest)
            .await
            .unwrap();
        // Crash immediately after rename: the publication name reopens as only the new generation.
        let new_reopen = rusqlite::Connection::open(&old_database).unwrap();
        assert_eq!(
            new_reopen
                .query_row("SELECT value FROM generation", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "new-main"
        );
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
