//! The `apiary` binary.
//!
//! - `apiary node run --config apiary.toml` runs a Node: the comb, the crop, the
//!   Flight SQL entrance and (if configured) the MQTT entrance.
//! - `apiary node check --config apiary.toml` checks a configuration file.
//! - `apiary sql "SELECT ..."` runs one query against a Node's entrance.
//!
//! A Node runs two Tokio runtimes. The CPU runtime has one worker thread per core
//! and runs the Node itself: queries, ingest, deposits, capping and harvest. The
//! I/O runtime serves the network entrances, so a long scan never delays a read
//! from a client. (Requests to the comb store still run on the CPU runtime.)

mod client;
mod config;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use apiary_entrance::{Guard, SetAside, flight, mqtt};
use apiary_runtime::ApiaryNode;

use crate::config::FileConfig;

#[derive(Parser)]
#[command(
    name = "apiary",
    version,
    about = "Apiary: a biomimetic data processing engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run and check a Node.
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    /// Run one query against a Node's Flight SQL entrance.
    Sql {
        /// The query.
        query: String,
        /// The Node's address.
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        url: String,
        /// The shared bearer token, if the Node requires one.
        #[arg(long, env = "APIARY_TOKEN")]
        token: Option<String>,
    },
}

#[derive(Subcommand)]
enum NodeCommand {
    /// Run a Node until interrupted.
    Run {
        /// The configuration file.
        #[arg(long, short)]
        config: PathBuf,
    },
    /// Check a configuration file and exit.
    Check {
        /// The configuration file.
        #[arg(long, short)]
        config: PathBuf,
    },
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let outcome = match Cli::parse().command {
        Command::Node {
            command: NodeCommand::Run { config },
        } => FileConfig::load(&config).and_then(run_node),
        Command::Node {
            command: NodeCommand::Check { config },
        } => FileConfig::load(&config).map(|c| {
            println!("{} is valid (comb: {})", config.display(), c.node.storage);
        }),
        Command::Sql { query, url, token } => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())
            .and_then(|rt| rt.block_on(client::sql(&url, token.as_deref(), &query))),
    };

    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("apiary: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run_node(file: FileConfig) -> Result<(), String> {
    let node_config = file.node_config();
    let cores = node_config.cores.max(1);

    let cpu = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores)
        .thread_name("apiary-cpu")
        .enable_all()
        .build()
        .map_err(|e| format!("Cannot start the CPU runtime: {e}"))?;
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("apiary-io")
        .enable_all()
        .build()
        .map_err(|e| format!("Cannot start the I/O runtime: {e}"))?;
    let io_handle = io.handle().clone();

    let result: Result<(), String> = cpu.block_on(async move {
        let node = Arc::new(
            ApiaryNode::start(node_config)
                .await
                .map_err(|e| format!("Cannot start the node: {e}"))?,
        );
        let set_aside = SetAside::open(node.config.set_aside_dir())
            .map_err(|e| format!("Cannot open the set-aside directory: {e}"))?;
        let guard = Guard::new(Arc::clone(&node), set_aside)
            .with_cpu_runtime(tokio::runtime::Handle::current());

        let flight_server = match &file.flight {
            Some(section) => {
                let token = section.resolve_token()?;
                if token.is_none() && !section.addr().ip().is_loopback() {
                    warn!(
                        listen = %section.addr(),
                        "The Flight entrance is open to the network with no token"
                    );
                }
                let guard = guard.clone();
                let addr = section.addr();
                let server = io_handle
                    .spawn(async move { flight::start(guard, addr, token).await })
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| format!("Cannot start the Flight entrance: {e}"))?;
                Some(server)
            }
            None => None,
        };

        let mqtt_subscriber = match file.mqtt.clone() {
            Some(mqtt_config) => {
                let guard = guard.clone();
                let subscriber = io_handle
                    .spawn(async move { mqtt::start(guard, mqtt_config) })
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| format!("Cannot start the MQTT entrance: {e}"))?;
                Some(subscriber)
            }
            None => None,
        };

        info!(node_id = %node.config.node_id, "Apiary node running; interrupt to stop");
        shutdown_signal().await;
        info!("Shutting down");

        // Stop taking deposits first, then secure what the node holds.
        if let Some(subscriber) = mqtt_subscriber {
            subscriber.stop().await;
        }
        if let Some(server) = flight_server {
            server.stop().await;
        }
        node.shutdown().await;
        Ok(())
    });

    io.shutdown_background();
    result
}

/// Wait for an interrupt, or on Unix a termination signal (what a container
/// runtime sends).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
