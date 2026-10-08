//! A Node's network: everything in this crate, assembled from one configuration.
//!
//! [`NetNode::start`] loads the Node's key, reads its join token, binds the QUIC
//! endpoint, starts the mesh that admits peers, serves the drive if this Node is
//! the comb host, runs discovery and the site monitor, and optionally hosts a
//! relay. [`NetNode::status`] reports what it sees.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use apiary_core::{ApiaryError, Clock, Result, StorageBackend, SystemClock};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::control::ControlRouter;
use crate::discovery::{
    Announcement, Discoverer, Discovery, DiscoveryHandle, Dns, DnsPeer, Mdns, Static,
    StoreRendezvous,
};
use crate::drive::{DRIVE_SERVICE, DriveService, DriveStore};
use crate::identity::{NodeId, NodeKey, parse_public};
use crate::iroh_transport::{IrohConfig, IrohTransport};
use crate::mesh::{Mesh, MeshConfig};
use crate::relay::{RelayServer, RelayTlsFiles};
use crate::revocation::RevocationStore;
use crate::site::{PROBE_SERVICE, ProbeService, SiteMonitor, SiteRules, Verdict};
use crate::token::{PeerHint, Token, Trust};
use crate::transport::{PathKind, PeerAddr, Protocol, Transport};

/// A peer reached by hostname (for Kubernetes headless Services).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DnsPeerConfig {
    /// The peer's Node id.
    pub id: String,
    /// Its hostname.
    pub host: String,
    /// The UDP port it listens on.
    pub port: u16,
}

/// A relay with TLS and QUIC address discovery, run in this Node.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RelayTlsConfig {
    /// The HTTPS port Nodes connect to.
    pub https: SocketAddr,
    /// The UDP port that answers QUIC address discovery.
    pub quic: SocketAddr,
    /// A plain-HTTP probe port (default: a loopback port nobody needs).
    #[serde(default)]
    pub http: Option<SocketAddr>,
    /// The certificate chain, as PEM.
    pub cert: PathBuf,
    /// The private key, as PEM.
    pub key: PathBuf,
}

/// The `[net]` configuration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NetConfig {
    /// The Apiary's name.
    pub apiary: String,
    /// The Apiary's public key (the one Beekeeper's `apiary key generate` prints).
    pub apiary_public_key: String,
    /// This Node's join token.
    #[serde(default)]
    pub token: Option<String>,
    /// Or a file holding it.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    /// Or an environment variable holding it.
    #[serde(default)]
    pub token_env: Option<String>,
    /// Where this Node's key lives (created on first start). Default:
    /// `<state dir>/node.key`. In Kubernetes, a mounted Secret.
    #[serde(default)]
    pub key_file: Option<PathBuf>,
    /// This Node's site label.
    #[serde(default)]
    pub site: Option<String>,
    /// The UDP port for QUIC (0 picks one). Pin it behind a firewall or a port mapping.
    #[serde(default)]
    pub udp_port: u16,
    /// Relay servers, as URLs. A Node behind NAT that nobody can dial directly
    /// needs one.
    #[serde(default)]
    pub relays: Vec<String>,
    /// Addresses to advertise besides the ones found, for a published port.
    #[serde(default)]
    pub external_addrs: Vec<SocketAddr>,
    /// Run a relay in this Node, listening here (plain HTTP: it forwards traffic
    /// but cannot help Nodes behind NATs find direct paths).
    #[serde(default)]
    pub serve_relay: Option<SocketAddr>,
    /// Run a relay with TLS and address discovery in this Node, which lets Nodes
    /// behind ordinary NATs punch through to direct paths.
    #[serde(default)]
    pub serve_relay_tls: Option<RelayTlsConfig>,
    /// A PEM file of certificates to trust for `https://` relays: a private
    /// relay's own certificate, or its CA.
    #[serde(default)]
    pub relay_ca: Option<PathBuf>,
    /// The UDP port TLS relays answer address discovery on (default 7842).
    #[serde(default)]
    pub relay_quic_port: Option<u16>,
    /// Announce and find peers with multicast DNS.
    #[serde(default = "yes")]
    pub mdns: bool,
    /// Announce and find peers through the comb (`floor/<node>`).
    #[serde(default = "yes")]
    pub rendezvous: bool,
    /// Peers to dial first.
    #[serde(default)]
    pub bootstrap: Vec<PeerHint>,
    /// Peers found by hostname.
    #[serde(default)]
    pub dns_peers: Vec<DnsPeerConfig>,
    /// Where the revocation list is kept. Default: `<state dir>/revocations.json`.
    #[serde(default)]
    pub revocations_file: Option<PathBuf>,
    /// This Node has the site's drive plugged in and serves it (`[node] storage`
    /// is the directory).
    #[serde(default)]
    pub serve_comb: bool,
    /// Seconds between discovery rounds.
    #[serde(default = "default_discovery")]
    pub discovery_interval_secs: u64,
    /// Seconds between site measurements.
    #[serde(default = "default_measure")]
    pub measure_interval_secs: u64,
}

fn yes() -> bool {
    true
}

fn default_discovery() -> u64 {
    5
}

fn default_measure() -> u64 {
    30
}

impl NetConfig {
    /// Where this Node's key is kept.
    pub fn key_path(&self, state_dir: &Path) -> PathBuf {
        self.key_file
            .clone()
            .unwrap_or_else(|| state_dir.join("node.key"))
    }
}

/// What [`NetNode::start`] needs from the Node around it.
pub struct NetContext {
    /// Where to keep this Node's key and revocation list unless the config says.
    pub state_dir: PathBuf,
    /// The directory of the site's comb, if this Node serves the drive.
    pub comb_dir: Option<PathBuf>,
    /// The comb as a rendezvous point, if it is available before the network is
    /// (the host's own directory is; a client's is the drive, added later).
    pub rendezvous: Option<Arc<dyn StorageBackend>>,
}

/// One admitted peer, as reported.
#[derive(Clone, Debug, Serialize)]
pub struct PeerStatus {
    /// The peer's Node id.
    pub id: String,
    /// Its colony.
    pub colony: String,
    /// Its declared site label.
    pub site: Option<String>,
    /// What its token lets it do.
    pub caps: String,
    /// `direct`, `relayed` or `unknown`.
    pub path: String,
    /// The round-trip time, in milliseconds.
    pub rtt_ms: Option<f64>,
    /// The probed throughput, in megabytes per second.
    pub throughput_mb_s: Option<f64>,
    /// `same-site`, `other-site`, `unknown`, or a contradiction message.
    pub verdict: String,
    /// Whether it counts as in this Node's site.
    pub same_site: bool,
    /// This Node's clock was behind the peer's token, so expiry was not checked.
    pub clock_suspect: bool,
}

/// A peer discovery has found, admitted or not.
#[derive(Clone, Debug, Serialize)]
pub struct DiscoveredStatus {
    /// The peer's Node id.
    pub id: String,
    /// Which sources found it.
    pub sources: Vec<String>,
    /// Whether this Node is admitted to it.
    pub connected: bool,
    /// Why the last dial failed.
    pub last_error: Option<String>,
}

/// What a Node reports about its network.
#[derive(Clone, Debug, Serialize)]
pub struct NetStatus {
    /// This Node's id.
    pub node_id: String,
    /// The Apiary's name.
    pub apiary: String,
    /// This Node's colony.
    pub colony: String,
    /// This Node's site label.
    pub site: Option<String>,
    /// Its direct addresses.
    pub addrs: Vec<String>,
    /// Its relay, if connected to one.
    pub relay: Option<String>,
    /// Whether it serves the site's drive.
    pub serves_comb: bool,
    /// The version of the revocation list it holds.
    pub revocations_seq: u64,
    /// The peers it is admitted to.
    pub peers: Vec<PeerStatus>,
    /// The peers discovery has found.
    pub discovered: Vec<DiscoveredStatus>,
}

/// A running network layer.
pub struct NetNode {
    mesh: Mesh,
    cfg: NetConfig,
    colony: String,
    discovery: DiscoveryHandle,
    monitor: Arc<SiteMonitor>,
    monitor_task: tokio::task::JoinHandle<()>,
    relay: Option<RelayServer>,
    serves_comb: bool,
    revocations: Arc<RevocationStore>,
    /// When this Node's join token was issued: a time that has certainly passed.
    token_issued_at: i64,
}

fn config_err(message: impl Into<String>) -> ApiaryError {
    ApiaryError::Config {
        message: message.into(),
    }
}

fn resolve_token(cfg: &NetConfig) -> Result<Token> {
    let text = if let Some(t) = &cfg.token {
        t.clone()
    } else if let Some(path) = &cfg.token_file {
        std::fs::read_to_string(path)
            .map_err(|e| ApiaryError::storage(format!("Failed to read {}", path.display()), e))?
    } else if let Some(var) = &cfg.token_env {
        std::env::var(var)
            .map_err(|_| config_err(format!("[net] token_env names {var}, which is not set")))?
    } else {
        return Err(config_err(
            "[net] needs a join token: set token, token_file or token_env",
        ));
    };
    Token::parse(&text).map_err(|e| config_err(format!("The join token is not valid: {e}")))
}

fn peer_hint(hint: &PeerHint) -> Result<PeerAddr> {
    Ok(PeerAddr {
        id: parse_public(&hint.id)?,
        direct: hint
            .addrs
            .iter()
            .map(|a| {
                a.parse::<SocketAddr>()
                    .map_err(|e| config_err(format!("Bootstrap address '{a}': {e}")))
            })
            .collect::<Result<Vec<_>>>()?,
        relay: None,
    })
}

impl NetNode {
    /// Start the network layer.
    pub async fn start(cfg: &NetConfig, ctx: NetContext) -> Result<Self> {
        let clock: Arc<dyn Clock> = SystemClock::shared();
        let key = NodeKey::load_or_create(&cfg.key_path(&ctx.state_dir))?;
        let trusted = parse_public(&cfg.apiary_public_key)?;
        let token = resolve_token(cfg)?;
        let colony = token.claims().colony.clone();

        let revocations = Arc::new(RevocationStore::open(
            Some(
                cfg.revocations_file
                    .clone()
                    .unwrap_or_else(|| ctx.state_dir.join("revocations.json")),
            ),
            trusted,
        ));

        // A relay of our own, if asked for; and the relays everyone is told about.
        let relay = match (&cfg.serve_relay_tls, cfg.serve_relay) {
            (Some(tls), _) => Some(
                RelayServer::spawn_tls(
                    tls.http
                        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0))),
                    &RelayTlsFiles {
                        https: tls.https,
                        quic: tls.quic,
                        cert: tls.cert.clone(),
                        key: tls.key.clone(),
                    },
                )
                .await?,
            ),
            (None, Some(bind)) => Some(RelayServer::spawn(bind).await?),
            (None, None) => None,
        };
        let mut relays = cfg.relays.clone();
        if let Some(url) = &token.claims().relay
            && !relays.contains(url)
        {
            relays.push(url.clone());
        }
        if relays.is_empty()
            && cfg.serve_relay_tls.is_none()
            && let Some(addr) = relay.as_ref().and_then(RelayServer::addr)
        {
            // The relay host uses its own relay when nobody gave another.
            relays.push(format!("http://127.0.0.1:{}", addr.port()));
        }

        // Peers named only by id are dialled through the relay everyone shares.
        let default_relay = relays.first().cloned();
        let mut iroh = IrohConfig::new(key.clone());
        iroh.udp_port = cfg.udp_port;
        iroh.relays = relays;
        iroh.external_addrs = cfg.external_addrs.clone();
        iroh.relay_quic_port = cfg.relay_quic_port;
        if let Some(ca) = &cfg.relay_ca {
            iroh.relay_ca = crate::relay::load_certs(ca)?;
        }
        let transport: Arc<dyn Transport> = Arc::new(IrohTransport::bind(iroh).await?);

        let mesh = Mesh::new(
            Arc::clone(&transport),
            MeshConfig {
                trust: Trust {
                    apiary: cfg.apiary.clone(),
                    key: trusted,
                },
                token: token.clone(),
                site: cfg.site.clone(),
                clock,
            },
            Arc::clone(&revocations),
        );

        let router = ControlRouter::new();
        router.add(PROBE_SERVICE, Arc::new(ProbeService));
        let mut serves_comb = false;
        if cfg.serve_comb {
            let dir = ctx.comb_dir.ok_or_else(|| {
                config_err("[net] serve_comb needs [node] storage to be a local directory")
            })?;
            let drive: Arc<dyn ObjectStore> =
                Arc::new(LocalFileSystem::new_with_prefix(&dir).map_err(|e| {
                    ApiaryError::storage(format!("Cannot serve {}", dir.display()), e)
                })?);
            router.add(DRIVE_SERVICE, Arc::new(DriveService::new(drive)));
            serves_comb = true;
            info!(dir = %dir.display(), "Serving the comb to the colony");
        }
        mesh.register(Protocol::Control, router);
        mesh.start();

        // Discovery: the token's and the config's bootstrap peers, DNS names,
        // mDNS, and the comb.
        let mut bootstrap: Vec<PeerAddr> = Vec::new();
        for hint in token.claims().bootstrap.iter().chain(&cfg.bootstrap) {
            let mut addr = peer_hint(hint)?;
            if addr.id != key.id() {
                addr.relay = default_relay.clone();
                bootstrap.push(addr);
            }
        }
        let mut sources: Vec<Arc<dyn Discovery>> = Vec::new();
        if !bootstrap.is_empty() {
            sources.push(Arc::new(Static::new(bootstrap)));
        }
        if !cfg.dns_peers.is_empty() {
            let peers = cfg
                .dns_peers
                .iter()
                .map(|p| {
                    Ok(DnsPeer {
                        id: parse_public(&p.id)?,
                        host: p.host.clone(),
                        port: p.port,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            sources.push(Arc::new(Dns::new(peers)));
        }
        if cfg.mdns {
            match Mdns::start() {
                Ok(mdns) => sources.push(Arc::new(mdns)),
                Err(e) => {
                    warn!(error = %e, "mDNS is not available; peers will be found by other means")
                }
            }
        }
        if cfg.rendezvous
            && let Some(store) = ctx.rendezvous
        {
            sources.push(Arc::new(StoreRendezvous::new(store)));
        }
        let discovery = Discoverer::new(
            mesh.clone(),
            sources,
            Announcement {
                addr: mesh.addr(),
                colony: colony.clone(),
                site: cfg.site.clone(),
            },
            Duration::from_secs(cfg.discovery_interval_secs.max(1)),
        )
        .spawn();

        let monitor = SiteMonitor::new(
            mesh.clone(),
            cfg.site.clone(),
            SiteRules::default(),
            256 * 1024,
        );
        let monitor_task = monitor.spawn(Duration::from_secs(cfg.measure_interval_secs.max(1)));

        info!(node_id = %key.id(), %colony, "Network started");
        Ok(Self {
            mesh,
            cfg: cfg.clone(),
            colony,
            discovery,
            monitor,
            monitor_task,
            relay,
            serves_comb,
            revocations,
            token_issued_at: token.claims().issued_at,
        })
    }

    /// This Node's id.
    pub fn id(&self) -> NodeId {
        self.mesh.id()
    }

    /// The gate that stops this Node committing while its clock is wrong: the
    /// clock must be past 2025 and not behind the time this Node's own join token
    /// was issued (allowing ten minutes of skew). Crops need no wall time, so
    /// ingest carries on; commits wait until the clock is right.
    pub fn commit_gate(&self) -> apiary_core::CommitGate {
        let floor = chrono::DateTime::from_timestamp(self.token_issued_at - 600, 0);
        Arc::new(move || apiary_core::check_clock(chrono::Utc::now(), floor))
    }

    /// The mesh, for the services that run over it.
    pub fn mesh(&self) -> &Mesh {
        &self.mesh
    }

    /// A store that reaches the drive on `host`, for `apiary-drive://` combs.
    pub fn drive_store(&self, host: NodeId) -> DriveStore {
        DriveStore::new(self.mesh.clone(), PeerAddr::id_only(host))
    }

    /// Use the comb as a rendezvous point too, once it is available.
    pub fn add_rendezvous(&self, store: Arc<dyn StorageBackend>) {
        if self.cfg.rendezvous {
            self.discovery
                .add_source(Arc::new(StoreRendezvous::new(store)));
        }
    }

    /// Wait until this Node is admitted to `peer`, up to `timeout`.
    pub async fn wait_for_peer(&self, peer: NodeId, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.mesh.peers().iter().any(|p| p.id == peer) {
                return true;
            }
            // Dial it ourselves rather than wait for the next discovery round.
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                self.mesh
                    .connect(&PeerAddr::id_only(peer), Protocol::Control),
            )
            .await;
            if tokio::time::Instant::now() >= deadline {
                return self.mesh.peers().iter().any(|p| p.id == peer);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Hand the mesh a revocation list from the Beekeeper.
    pub fn apply_revocations(&self, list: crate::revocation::Revocations) -> Result<bool> {
        self.mesh.apply_revocations(list).map_err(Into::into)
    }

    /// What this Node sees of its network.
    pub fn status(&self) -> NetStatus {
        let sites = self.monitor.view();
        let peers = self
            .mesh
            .peers()
            .into_iter()
            .map(|p| {
                let site = sites.iter().find(|s| s.id == p.id);
                let measured = site.map(|s| s.measured);
                let path = measured.map_or(p.path.kind, |m| m.path);
                PeerStatus {
                    id: p.id.to_string(),
                    colony: p.membership.colony.clone(),
                    site: p.site.clone(),
                    caps: p.membership.caps.to_string(),
                    path: match path {
                        PathKind::Direct => "direct",
                        PathKind::Relayed => "relayed",
                        PathKind::Unknown => "unknown",
                    }
                    .to_string(),
                    rtt_ms: measured
                        .and_then(|m| m.rtt)
                        .or(p.path.rtt)
                        .map(|r| r.as_secs_f64() * 1000.0),
                    throughput_mb_s: measured.and_then(|m| m.throughput).map(|t| t / 1_000_000.0),
                    verdict: match site.map(|s| &s.verdict) {
                        Some(Verdict::SameSite) => "same-site".to_string(),
                        Some(Verdict::OtherSite) => "other-site".to_string(),
                        Some(Verdict::Contradiction(why)) => format!("contradiction: {why}"),
                        _ => "unknown".to_string(),
                    },
                    same_site: site.is_some_and(|s| s.same_site),
                    clock_suspect: p.membership.clock_suspect,
                }
            })
            .collect();
        let addr = self.mesh.addr();
        NetStatus {
            node_id: self.id().to_string(),
            apiary: self.cfg.apiary.clone(),
            colony: self.colony.clone(),
            site: self.cfg.site.clone(),
            addrs: addr.direct.iter().map(ToString::to_string).collect(),
            relay: addr.relay,
            serves_comb: self.serves_comb,
            revocations_seq: self.revocations.current().seq,
            peers,
            discovered: self
                .discovery
                .known()
                .into_iter()
                .map(|k| DiscoveredStatus {
                    id: k.addr.id.to_string(),
                    sources: k.sources.iter().map(ToString::to_string).collect(),
                    connected: k.connected,
                    last_error: k.last_error,
                })
                .collect(),
        }
    }

    /// Stop everything: withdraw from discovery, close connections, stop the relay.
    pub async fn shutdown(self) {
        self.monitor_task.abort();
        self.discovery.stop().await;
        self.mesh.transport().close().await;
        if let Some(relay) = self.relay {
            relay.shutdown().await;
        }
    }
}

/// A path under a state directory, for callers that want the defaults.
pub fn default_state_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("net")
}
