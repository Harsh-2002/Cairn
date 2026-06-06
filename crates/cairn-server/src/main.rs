//! The Cairn server binary entrypoint. Parses configuration, initialises observability, and
//! runs the HTTP server with ordered graceful shutdown. The full CLI (remote administration
//! and node-local commands) ships as `cairn-cli` in a later wave; this binary carries `serve`
//! and `validate-config` so the server stands up and configuration can be checked.

#![forbid(unsafe_code)]

mod config;
mod observability;
mod server;

use clap::{Parser, Subcommand};
use config::Config;
use std::path::PathBuf;
use std::process::ExitCode;

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
        Command::Serve => run_server(cfg),
    }
}

fn run_server(cfg: Config) -> ExitCode {
    observability::init_tracing(&cfg.log_level, cfg.log_format);
    let metrics = observability::init_metrics();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(server::serve(cfg, metrics)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "server exited with error");
            ExitCode::FAILURE
        }
    }
}
