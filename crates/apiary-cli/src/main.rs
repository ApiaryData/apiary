//! The `apiary` binary.
//!
//! A Node:
//! - `apiary node run --config apiary.toml` runs a Node: the comb, the crop, the
//!   network (QUIC, membership, discovery, the drive), the Flight SQL entrance
//!   and the MQTT entrance, as configured.
//! - `apiary node check --config apiary.toml` checks a configuration file.
//! - `apiary node id --config apiary.toml` prints the Node's id (creating its key).
//! - `apiary sql "SELECT ..."` runs one query against a Node's entrance.
//! - `apiary net status` shows what a Node sees of its network.
//!
//! The Beekeeper (who holds the Apiary's signing key):
//! - `apiary key generate|public`, `apiary token issue|inspect`, `apiary revoke`.
//!
//! A relay for a cloud VM: `apiary relay run`.
//!
//! A Node runs two Tokio runtimes. The CPU runtime has one worker thread per core
//! and runs the Node itself: queries, ingest, deposits, capping and harvest. The
//! I/O runtime carries the network: the QUIC endpoint and the Flight and MQTT
//! entrances, so a long scan never delays a read from a client or a gossip round.
//! (Requests to an S3 or local comb still run on the CPU runtime.)

mod beekeeper;
mod client;
mod config;
mod net_cmd;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use apiary_comb::LocalBackend;
use apiary_comb::custom_store::{drive_authority, register_store};
use apiary_comb::local::expand_local_path;
use apiary_core::StorageBackend;
use apiary_entrance::flight::{FlightExtras, start_with};
use apiary_entrance::{Guard, SetAside, mqtt};
use apiary_net::{NetContext, NetNode, Revocations, default_state_dir, parse_public};
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
    /// What a Node sees of its network.
    Net {
        #[command(subcommand)]
        command: NetCommand,
    },
    /// The Apiary's signing key (the Beekeeper's).
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
    /// Join tokens (the Beekeeper's).
    Token {
        #[command(subcommand)]
        command: TokenCommand,
    },
    /// Revoke a Node's key or a token, and sign the new list (the Beekeeper's).
    Revoke {
        /// The Apiary key file.
        #[arg(long)]
        key: PathBuf,
        /// The revocation list file: read if it exists, then written back.
        #[arg(long, default_value = "revocations.json")]
        list: PathBuf,
        /// A Node id to revoke (repeatable).
        #[arg(long)]
        node: Vec<String>,
        /// A token id to revoke (repeatable).
        #[arg(long)]
        token_id: Vec<String>,
        /// Hand the list to this running Node, which passes it on.
        #[arg(long)]
        push: Option<String>,
        /// The shared bearer token of the Node you push to, if it needs one.
        #[arg(long, env = "APIARY_TOKEN")]
        flight_token: Option<String>,
    },
    /// Run a relay with no Node attached, for a cloud VM.
    Relay {
        #[command(subcommand)]
        command: RelayCommand,
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
    /// Print the Node's id, creating its key if it has none.
    Id {
        /// The configuration file.
        #[arg(long, short)]
        config: PathBuf,
    },
}

#[derive(Subcommand)]
enum NetCommand {
    /// Show peers, paths and sites as a Node sees them.
    Status {
        /// The Node's address.
        #[arg(long, default_value = "http://127.0.0.1:50051")]
        url: String,
        /// The shared bearer token, if the Node requires one.
        #[arg(long, env = "APIARY_TOKEN")]
        token: Option<String>,
        /// Print the raw JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum KeyCommand {
    /// Make the Apiary's signing key.
    Generate {
        /// Where to write it (it must not exist).
        #[arg(long)]
        out: PathBuf,
    },
    /// Print the public half of an Apiary key.
    Public {
        /// The Apiary key file.
        #[arg(long)]
        key: PathBuf,
    },
}

#[derive(Subcommand)]
enum TokenCommand {
    /// Sign a join token for a Node.
    Issue {
        /// The Apiary key file.
        #[arg(long)]
        key: PathBuf,
        /// The Apiary's name.
        #[arg(long)]
        apiary: String,
        /// The colony the Node joins.
        #[arg(long)]
        colony: String,
        /// What the Node may do: run, ingest, read.
        #[arg(long, default_value = "run,ingest")]
        caps: String,
        /// How long the token lasts, in days.
        #[arg(long, default_value_t = 365)]
        days: i64,
        /// Bind the token to one Node id.
        #[arg(long)]
        node: Option<String>,
        /// A peer to dial first, as id@host:port (repeatable).
        #[arg(long)]
        bootstrap: Vec<String>,
        /// A relay to use, as a URL.
        #[arg(long)]
        relay: Option<String>,
    },
    /// Show a token's claims, and check it if the Apiary's public key is given.
    Inspect {
        /// The token.
        token: String,
        /// The Apiary's public key.
        #[arg(long)]
        public: Option<String>,
    },
}

#[derive(Subcommand)]
enum RelayCommand {
    /// Serve a relay. Plain HTTP by default; with --cert and --key, TLS with QUIC
    /// address discovery, which lets Nodes behind NATs find direct paths.
    Run {
        /// Where to listen for plain HTTP (with TLS: a probe port).
        #[arg(long, default_value = "0.0.0.0:3340")]
        listen: String,
        /// Where to listen for HTTPS (needs --cert and --key).
        #[arg(long, default_value = "0.0.0.0:3341")]
        https: String,
        /// Where to listen for QUIC address discovery (UDP).
        #[arg(long, default_value = "0.0.0.0:7842")]
        quic: String,
        /// The relay's certificate chain, as PEM.
        #[arg(long, requires = "key")]
        cert: Option<PathBuf>,
        /// The relay's private key, as PEM.
        #[arg(long, requires = "cert")]
        key: Option<PathBuf>,
    },
    /// Make a self-signed certificate for a relay. Give the certificate to Nodes
    /// as `relay_ca` so they trust it.
    Cert {
        /// A directory for relay.pem and relay.key.
        #[arg(long)]
        out: PathBuf,
        /// A DNS name or IP address the relay is reached at (repeatable).
        #[arg(long = "name", required = true)]
        names: Vec<String>,
    },
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let outcome = dispatch(Cli::parse().command);
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("apiary: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Run a small async command on its own current-thread runtime.
fn block_on<F: std::future::Future<Output = Result<(), String>>>(fut: F) -> Result<(), String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?
        .block_on(fut)
}

fn dispatch(command: Command) -> Result<(), String> {
    match command {
        Command::Node {
            command: NodeCommand::Run { config },
        } => FileConfig::load(&config).and_then(run_node),
        Command::Node {
            command: NodeCommand::Check { config },
        } => FileConfig::load(&config).map(|c| {
            println!("{} is valid (comb: {})", config.display(), c.node.storage);
        }),
        Command::Node {
            command: NodeCommand::Id { config },
        } => {
            let file = FileConfig::load(&config)?;
            let net = file
                .net
                .as_ref()
                .ok_or("This configuration has no [net] section, so the node has no key")?;
            let state = default_state_dir(&file.node_config().cache_dir);
            let key = apiary_net::NodeKey::load_or_create(&net.key_path(&state))
                .map_err(|e| e.to_string())?;
            println!("{}", key.id());
            Ok(())
        }
        Command::Sql { query, url, token } => block_on(client::sql(&url, token.as_deref(), &query)),
        Command::Net {
            command: NetCommand::Status { url, token, json },
        } => block_on(net_cmd::status(&url, token.as_deref(), json)),
        Command::Key {
            command: KeyCommand::Generate { out },
        } => beekeeper::key_generate(&out),
        Command::Key {
            command: KeyCommand::Public { key },
        } => beekeeper::key_public(&key),
        Command::Token {
            command:
                TokenCommand::Issue {
                    key,
                    apiary,
                    colony,
                    caps,
                    days,
                    node,
                    bootstrap,
                    relay,
                },
        } => beekeeper::token_issue(&beekeeper::IssueArgs {
            key: &key,
            apiary: &apiary,
            colony: &colony,
            caps: &caps,
            days,
            node: node.as_deref(),
            bootstrap: &bootstrap,
            relay: relay.as_deref(),
        }),
        Command::Token {
            command: TokenCommand::Inspect { token, public },
        } => beekeeper::token_inspect(&token, public.as_deref()),
        Command::Revoke {
            key,
            list,
            node,
            token_id,
            push,
            flight_token,
        } => {
            let next = beekeeper::revoke(&key, &list, &node, &token_id)?;
            match push {
                Some(url) => block_on(async {
                    net_cmd::push_revocations(&url, flight_token.as_deref(), &next).await
                }),
                None => Ok(()),
            }
        }
        Command::Relay {
            command:
                RelayCommand::Run {
                    listen,
                    https,
                    quic,
                    cert,
                    key,
                },
        } => block_on(net_cmd::relay(
            &listen,
            cert.zip(key).map(|(cert, key)| (https, quic, cert, key)),
        )),
        Command::Relay {
            command: RelayCommand::Cert { out, names },
        } => net_cmd::relay_cert(&out, &names),
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
        // The network comes first: a Node whose comb is another Node's drive
        // cannot start until it can reach that drive.
        let net = match &file.net {
            Some(net_config) => {
                let state_dir = default_state_dir(&node_config.cache_dir);
                let comb_dir = if net_config.serve_comb {
                    let uri = &node_config.storage_uri;
                    if uri.starts_with("s3://") || drive_authority(uri).is_some() {
                        return Err(
                            "[net] serve_comb needs [node] storage to be a local directory"
                                .to_string(),
                        );
                    }
                    let path = uri.strip_prefix("local://").unwrap_or(uri);
                    Some(expand_local_path(path).map_err(|e| e.to_string())?)
                } else {
                    None
                };
                // The host can use its own comb as a rendezvous right away.
                let rendezvous: Option<Arc<dyn StorageBackend>> = match &comb_dir {
                    Some(dir) => Some(Arc::new(
                        LocalBackend::new(dir.clone())
                            .await
                            .map_err(|e| e.to_string())?,
                    )),
                    None => None,
                };
                let net_config = net_config.clone();
                let net = io_handle
                    .spawn(async move {
                        NetNode::start(
                            &net_config,
                            NetContext {
                                state_dir,
                                comb_dir,
                                rendezvous,
                            },
                        )
                        .await
                    })
                    .await
                    .map_err(|e| e.to_string())?
                    .map_err(|e| format!("Cannot start the network: {e}"))?;
                let net = Arc::new(net);

                // A comb on another Node: register the drive, then wait for the host.
                if let Some(authority) = drive_authority(&node_config.storage_uri) {
                    let host = parse_public(&authority).map_err(|e| e.to_string())?;
                    register_store(&authority, Arc::new(net.drive_store(host)));
                    info!(host = %host.fmt_short(), "Waiting for the comb host");
                    if !net.wait_for_peer(host, Duration::from_secs(90)).await {
                        warn!("The comb host has not answered yet; the node will keep trying");
                    }
                }
                Some(net)
            }
            None => None,
        };

        // A Node with no trustworthy clock keeps ingesting but will not commit.
        let gate = net.as_ref().map(|n| n.commit_gate());
        let node = Arc::new(
            ApiaryNode::start_with_gate(node_config, apiary_core::Env::system(), gate)
                .await
                .map_err(|e| format!("Cannot start the node: {e}"))?,
        );
        if let (Some(net), Some(net_config)) = (&net, &file.net)
            && !net_config.serve_comb
        {
            // Now the comb is up, it can carry the rendezvous too.
            net.add_rendezvous(Arc::clone(&node.storage));
        }
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
                let extras = match &net {
                    Some(net) => {
                        let status_net = Arc::clone(net);
                        let revoke_net = Arc::clone(net);
                        FlightExtras {
                            net_status: Some(Arc::new(move || {
                                serde_json::to_value(status_net.status()).unwrap_or_default()
                            })),
                            revoke: Some(Arc::new(move |value| {
                                let list: Revocations =
                                    serde_json::from_value(value).map_err(|e| e.to_string())?;
                                revoke_net
                                    .apply_revocations(list)
                                    .map_err(|e| e.to_string())
                            })),
                        }
                    }
                    None => FlightExtras::default(),
                };
                let guard = guard.clone();
                let addr = section.addr();
                let server = io_handle
                    .spawn(async move { start_with(guard, addr, token, extras).await })
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

        // Stop taking deposits first, then secure what the node holds, and only
        // then leave the network (the deposit may need the drive).
        if let Some(subscriber) = mqtt_subscriber {
            subscriber.stop().await;
        }
        if let Some(server) = flight_server {
            server.stop().await;
        }
        node.shutdown().await;
        if let Some(net) = net.and_then(|n| Arc::try_unwrap(n).ok()) {
            net.shutdown().await;
        }
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
