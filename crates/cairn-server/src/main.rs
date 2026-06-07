//! The Cairn server binary entrypoint. Parses configuration, initialises observability, builds
//! the engine stack, and runs the HTTP server with ordered graceful shutdown. Also carries the
//! node-local `bootstrap` command that mints the first administrator. The full remote-admin CLI
//! ships as `cairn-cli` in a later wave.

#![forbid(unsafe_code)]

mod adapter;
mod background;
mod config;
mod observability;
mod server;
mod stack;

use clap::{Parser, Subcommand};
use config::Config;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

/// Cairn — a production-grade, S3-compatible object storage server.
#[derive(Debug, Parser)]
#[command(name = "cairn", version, about)]
struct Cli {
    /// Path to an optional TOML configuration file.
    #[arg(long, global = true, env = "CAIRN_CONFIG")]
    config: Option<PathBuf>,
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
    /// Create the first administrator into an empty store and print its credentials once.
    Bootstrap,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let cfg = match Config::load(cli.config.as_ref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            return ExitCode::from(2);
        }
    };

    match cli.command.unwrap_or(Command::Serve) {
        Command::ValidateConfig => {
            println!("configuration valid");
            ExitCode::SUCCESS
        }
        Command::Bootstrap => bootstrap(cfg),
        Command::Serve => run_server(cfg),
    }
}

fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}

fn run_server(cfg: Config) -> ExitCode {
    observability::init_tracing(&cfg.log_level, cfg.log_format);
    let metrics = observability::init_metrics();

    let rt = match runtime() {
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
    use cairn_types::auth::Role;
    use cairn_types::id::UserId;
    use cairn_types::meta::{Mutation, User, UserRecord};
    use cairn_types::traits::{Clock, Crypto, MetadataStore};

    let rt = match runtime() {
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

        let store = match cairn_meta::open(&cfg.db_path, &cairn_meta::OpenOptions::default()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to open metadata store: {e}");
                return ExitCode::FAILURE;
            }
        };
        match store.count_users().await {
            Ok(0) => {}
            Ok(_) => {
                eprintln!("a user already exists; refusing to bootstrap again");
                return ExitCode::from(1);
            }
            Err(e) => {
                eprintln!("failed to query users: {e}");
                return ExitCode::FAILURE;
            }
        }

        let crypto = match stack::build_crypto(&cfg) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };
        let clock = cairn_crypto::SystemClock::new();
        let now = clock.now();

        let bearer_akid = format!("cairn_{}", uuid::Uuid::new_v4().simple());
        let bearer_secret = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let sigv4_akid = format!(
            "AKIA{}",
            &uuid::Uuid::new_v4().simple().to_string()[..16].to_uppercase()
        );
        let sigv4_secret = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );

        let sealed = match crypto.seal(sigv4_secret.as_bytes()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to seal SigV4 secret: {e}");
                return ExitCode::FAILURE;
            }
        };

        let record = UserRecord {
            user: User {
                id: UserId::generate(),
                display_name: "administrator".to_owned(),
                access_key_id: bearer_akid.clone(),
                sigv4_access_key_id: Some(sigv4_akid.clone()),
                role: Role::Administrator,
                is_active: true,
                created_at: now,
                updated_at: now,
            },
            bearer_secret_hash: cairn_auth::hash_bearer_secret(&bearer_secret),
            sigv4_secret_ciphertext: Some(sealed.ciphertext),
            sigv4_secret_nonce: Some(sealed.nonce.0),
        };

        if let Err(e) = store.submit(Mutation::CreateUser(Box::new(record))).await {
            eprintln!("failed to create administrator: {e}");
            return ExitCode::FAILURE;
        }

        println!("Administrator created. Save these credentials now — they are shown only once.\n");
        println!("  Bearer:");
        println!("    Authorization: Bearer {bearer_akid}.{bearer_secret}\n");
        println!("  SigV4 (S3 SDKs / aws-cli):");
        println!("    Access Key Id:     {sigv4_akid}");
        println!("    Secret Access Key: {sigv4_secret}");
        println!("    Region:            {}", cfg.region);
        ExitCode::SUCCESS
    })
}
