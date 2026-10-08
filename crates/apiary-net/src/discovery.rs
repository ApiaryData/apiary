//! Discovery: how Nodes find each other.
//!
//! | Where | Source |
//! |---|---|
//! | First contact | [`Static`]: the bootstrap peers and relay in a join token or the config |
//! | A LAN | [`Mdns`]: announcements of Node id and colony, no configuration |
//! | Kubernetes | [`Dns`]: a headless Service's name resolves to every peer pod's address |
//! | Anywhere | [`StoreRendezvous`]: each Node writes `floor/<node>` to the site's comb, and any Node that can read it finds the rest |
//!
//! A source only says where peers might be. It never vouches for them: the
//! [`Discoverer`] dials what the sources find, and membership (a valid token for
//! the key the peer connected as) decides who is admitted. A forged or stale
//! announcement costs one failed dial.
//!
//! Kubernetes note: a headless Service resolves to pod addresses, not Node ids,
//! and a Node is dialled by id. So [`Dns`] is told each peer's id (the keys live
//! in Secrets, so a rescheduled pod keeps its id) and fills in the addresses.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use apiary_core::StorageBackend;

use crate::error::NetError;
use crate::identity::{NodeId, parse_public};
use crate::mesh::Mesh;
use crate::transport::{PeerAddr, Protocol};

/// What a Node says about itself.
#[derive(Clone, Debug)]
pub struct Announcement {
    /// Where it can be reached.
    pub addr: PeerAddr,
    /// Its colony.
    pub colony: String,
    /// Its declared site label.
    pub site: Option<String>,
}

/// A peer a source found.
#[derive(Clone, Debug)]
pub struct Found {
    /// Where it might be reached.
    pub addr: PeerAddr,
    /// The colony it announced, if the source says.
    pub colony: Option<String>,
    /// The site label it announced, if the source says.
    pub site: Option<String>,
}

/// Somewhere Nodes announce themselves and look for each other.
#[async_trait]
pub trait Discovery: Send + Sync {
    /// A name for logs.
    fn name(&self) -> &'static str;

    /// Announce this Node. Sources that only look (DNS, static) do nothing.
    async fn announce(&self, me: &Announcement) -> Result<(), NetError>;

    /// Withdraw the announcement, at shutdown.
    async fn withdraw(&self, _me: &Announcement) -> Result<(), NetError> {
        Ok(())
    }

    /// The peers this source knows of now.
    async fn find(&self) -> Result<Vec<Found>, NetError>;
}

/// Peers named in advance: a join token's bootstrap list, or the config.
pub struct Static {
    peers: Vec<PeerAddr>,
}

impl Static {
    /// A source that always returns these peers.
    pub fn new(peers: Vec<PeerAddr>) -> Self {
        Self { peers }
    }
}

#[async_trait]
impl Discovery for Static {
    fn name(&self) -> &'static str {
        "bootstrap"
    }

    async fn announce(&self, _me: &Announcement) -> Result<(), NetError> {
        Ok(())
    }

    async fn find(&self) -> Result<Vec<Found>, NetError> {
        Ok(self
            .peers
            .iter()
            .cloned()
            .map(|addr| Found {
                addr,
                colony: None,
                site: None,
            })
            .collect())
    }
}

/// A peer reached by hostname, such as a Kubernetes StatefulSet pod behind a
/// headless Service.
#[derive(Clone, Debug)]
pub struct DnsPeer {
    /// The peer's Node id.
    pub id: NodeId,
    /// Its hostname (`apiary-1.apiary.default.svc`).
    pub host: String,
    /// The UDP port it listens on.
    pub port: u16,
}

/// Resolves peer hostnames to addresses.
pub struct Dns {
    peers: Vec<DnsPeer>,
}

impl Dns {
    /// A source that resolves each peer's hostname when asked.
    pub fn new(peers: Vec<DnsPeer>) -> Self {
        Self { peers }
    }
}

#[async_trait]
impl Discovery for Dns {
    fn name(&self) -> &'static str {
        "dns"
    }

    async fn announce(&self, _me: &Announcement) -> Result<(), NetError> {
        Ok(())
    }

    async fn find(&self) -> Result<Vec<Found>, NetError> {
        let mut found = Vec::new();
        for peer in &self.peers {
            // A pod that is not up yet has no DNS record: not an error.
            let Ok(addrs) = tokio::net::lookup_host((peer.host.as_str(), peer.port)).await else {
                continue;
            };
            found.push(Found {
                addr: PeerAddr {
                    id: peer.id,
                    direct: addrs.collect(),
                    relay: None,
                },
                colony: None,
                site: None,
            });
        }
        Ok(found)
    }
}

/// What a Node writes to `floor/<node>`.
#[derive(Serialize, Deserialize)]
struct FloorEntry {
    id: String,
    addrs: Vec<SocketAddr>,
    relay: Option<String>,
    colony: String,
    site: Option<String>,
}

/// The comb as a rendezvous point: each Node writes its id and addresses under
/// `floor/`, and any Node that can read the store finds the others. At a site
/// the store is the comb on the drive; across sites it is the harvest bucket.
pub struct StoreRendezvous {
    store: Arc<dyn StorageBackend>,
}

impl StoreRendezvous {
    /// Rendezvous through `store`.
    pub fn new(store: Arc<dyn StorageBackend>) -> Self {
        Self { store }
    }

    fn key(id: &NodeId) -> String {
        format!("floor/{id}")
    }
}

fn store_err(e: apiary_core::ApiaryError) -> NetError {
    NetError::Unreachable(format!("the rendezvous store: {e}"))
}

#[async_trait]
impl Discovery for StoreRendezvous {
    fn name(&self) -> &'static str {
        "store"
    }

    async fn announce(&self, me: &Announcement) -> Result<(), NetError> {
        let entry = FloorEntry {
            id: me.addr.id.to_string(),
            addrs: me.addr.direct.clone(),
            relay: me.addr.relay.clone(),
            colony: me.colony.clone(),
            site: me.site.clone(),
        };
        let bytes = serde_json::to_vec(&entry).map_err(|e| NetError::Protocol(e.to_string()))?;
        self.store
            .put(&Self::key(&me.addr.id), Bytes::from(bytes))
            .await
            .map_err(store_err)
    }

    async fn withdraw(&self, me: &Announcement) -> Result<(), NetError> {
        self.store
            .delete(&Self::key(&me.addr.id))
            .await
            .map_err(store_err)
    }

    async fn find(&self) -> Result<Vec<Found>, NetError> {
        let mut found = Vec::new();
        for key in self.store.list("floor/").await.map_err(store_err)? {
            let Ok(bytes) = self.store.get(&key).await else {
                continue;
            };
            let Ok(entry) = serde_json::from_slice::<FloorEntry>(&bytes) else {
                continue;
            };
            let Ok(id) = parse_public(&entry.id) else {
                continue;
            };
            found.push(Found {
                addr: PeerAddr {
                    id,
                    direct: entry.addrs,
                    relay: entry.relay,
                },
                colony: Some(entry.colony),
                site: entry.site,
            });
        }
        Ok(found)
    }
}

const MDNS_TYPE: &str = "_apiary._udp.local.";

/// LAN discovery with multicast DNS: Nodes announce their id, colony and port,
/// and find each other with no configuration.
pub struct Mdns {
    daemon: mdns_sd::ServiceDaemon,
    seen: Arc<Mutex<HashMap<NodeId, Found>>>,
    registered: Mutex<Option<String>>,
}

impl Mdns {
    /// Start the mDNS daemon and begin browsing.
    pub fn start() -> Result<Self, NetError> {
        let daemon = mdns_sd::ServiceDaemon::new()
            .map_err(|e| NetError::Unreachable(format!("cannot start mDNS: {e}")))?;
        let browse = daemon
            .browse(MDNS_TYPE)
            .map_err(|e| NetError::Unreachable(format!("cannot browse mDNS: {e}")))?;
        let seen: Arc<Mutex<HashMap<NodeId, Found>>> = Arc::default();

        let table = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok(event) = browse.recv_async().await {
                match event {
                    mdns_sd::ServiceEvent::ServiceResolved(info) => {
                        let Some(id) = info
                            .get_property_val_str("id")
                            .and_then(|s| parse_public(s).ok())
                        else {
                            continue;
                        };
                        let port = info.get_port();
                        let direct: Vec<SocketAddr> = info
                            .get_addresses()
                            .iter()
                            .map(|ip| SocketAddr::new(ip.to_ip_addr(), port))
                            .collect();
                        let found = Found {
                            addr: PeerAddr {
                                id,
                                direct,
                                relay: info.get_property_val_str("relay").map(String::from),
                            },
                            colony: info.get_property_val_str("colony").map(String::from),
                            site: info.get_property_val_str("site").map(String::from),
                        };
                        table.lock().expect("mdns table poisoned").insert(id, found);
                    }
                    mdns_sd::ServiceEvent::ServiceRemoved(_, fullname) => {
                        // The instance name starts with the short id.
                        let name = fullname.split('.').next().unwrap_or_default().to_string();
                        table
                            .lock()
                            .expect("mdns table poisoned")
                            .retain(|id, _| id.fmt_short().to_string() != name);
                    }
                    _ => {}
                }
            }
        });

        Ok(Self {
            daemon,
            seen,
            registered: Mutex::new(None),
        })
    }
}

#[async_trait]
impl Discovery for Mdns {
    fn name(&self) -> &'static str {
        "mdns"
    }

    async fn announce(&self, me: &Announcement) -> Result<(), NetError> {
        // The QUIC port is the same on every address; take the first direct one.
        let Some(port) = me.addr.direct.first().map(SocketAddr::port) else {
            return Ok(());
        };
        let instance = me.addr.id.fmt_short().to_string();
        let mut props = vec![
            ("id".to_string(), me.addr.id.to_string()),
            ("colony".to_string(), me.colony.clone()),
        ];
        if let Some(site) = &me.site {
            props.push(("site".to_string(), site.clone()));
        }
        if let Some(relay) = &me.addr.relay {
            props.push(("relay".to_string(), relay.clone()));
        }
        let host = format!("{instance}.local.");
        let info = mdns_sd::ServiceInfo::new(MDNS_TYPE, &instance, &host, "", port, &props[..])
            .map_err(|e| NetError::Protocol(format!("bad mDNS record: {e}")))?
            .enable_addr_auto();
        let fullname = info.get_fullname().to_string();
        self.daemon
            .register(info)
            .map_err(|e| NetError::Unreachable(format!("cannot announce on mDNS: {e}")))?;
        *self.registered.lock().expect("mdns poisoned") = Some(fullname);
        Ok(())
    }

    async fn withdraw(&self, _me: &Announcement) -> Result<(), NetError> {
        if let Some(fullname) = self.registered.lock().expect("mdns poisoned").take() {
            let _ = self.daemon.unregister(&fullname);
        }
        Ok(())
    }

    async fn find(&self) -> Result<Vec<Found>, NetError> {
        Ok(self
            .seen
            .lock()
            .expect("mdns table poisoned")
            .values()
            .cloned()
            .collect())
    }
}

impl Drop for Mdns {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}

/// What the [`Discoverer`] knows about a peer it has found.
#[derive(Clone, Debug)]
pub struct Known {
    /// Where it was found.
    pub addr: PeerAddr,
    /// The colony it announced.
    pub colony: Option<String>,
    /// The site label it announced.
    pub site: Option<String>,
    /// Which sources found it.
    pub sources: Vec<&'static str>,
    /// Whether this Node is admitted to it now.
    pub connected: bool,
    /// Why the last attempt failed.
    pub last_error: Option<String>,
}

struct Tracked {
    known: Known,
    next_try: Instant,
    failures: u32,
}

/// Announces this Node, finds peers through every source, and dials them.
pub struct Discoverer {
    mesh: Mesh,
    sources: Arc<Mutex<Vec<Arc<dyn Discovery>>>>,
    me: Announcement,
    interval: Duration,
    state: Arc<Mutex<HashMap<NodeId, Tracked>>>,
}

/// A running [`Discoverer`].
pub struct DiscoveryHandle {
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    state: Arc<Mutex<HashMap<NodeId, Tracked>>>,
    sources: Arc<Mutex<Vec<Arc<dyn Discovery>>>>,
}

impl DiscoveryHandle {
    /// Start using another source from the next round. A Node that reaches its
    /// comb through the drive can only use the comb as a rendezvous once the
    /// drive is up.
    pub fn add_source(&self, source: Arc<dyn Discovery>) {
        self.sources
            .lock()
            .expect("discovery poisoned")
            .push(source);
    }

    /// The peers found so far.
    pub fn known(&self) -> Vec<Known> {
        let mut known: Vec<Known> = self
            .state
            .lock()
            .expect("discovery poisoned")
            .values()
            .map(|t| t.known.clone())
            .collect();
        known.sort_by_key(|k| k.addr.id);
        known
    }

    /// Stop, withdrawing the announcement.
    pub async fn stop(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
    }
}

impl Discoverer {
    /// A discoverer for `mesh`, running `sources` every `interval`.
    pub fn new(
        mesh: Mesh,
        sources: Vec<Arc<dyn Discovery>>,
        me: Announcement,
        interval: Duration,
    ) -> Self {
        Self {
            mesh,
            sources: Arc::new(Mutex::new(sources)),
            me,
            interval,
            state: Arc::default(),
        }
    }

    /// Start running.
    pub fn spawn(self) -> DiscoveryHandle {
        let (stop, mut stopped) = watch::channel(false);
        let state = Arc::clone(&self.state);
        let sources = Arc::clone(&self.sources);
        let task = tokio::spawn(async move {
            loop {
                self.round().await;
                tokio::select! {
                    _ = tokio::time::sleep(self.interval) => {}
                    _ = stopped.wait_for(|s| *s) => break,
                }
            }
            let sources: Vec<_> = self.sources.lock().expect("discovery poisoned").clone();
            for source in &sources {
                let _ = source.withdraw(&self.me).await;
            }
        });
        DiscoveryHandle {
            stop,
            task,
            state,
            sources,
        }
    }

    /// One pass: announce, find, dial.
    async fn round(&self) {
        // Our own addresses change (relay connected, port mapped): announce afresh.
        let mut me = self.me.clone();
        me.addr = self.mesh.addr();
        let sources: Vec<Arc<dyn Discovery>> =
            self.sources.lock().expect("discovery poisoned").clone();
        for source in &sources {
            if let Err(e) = source.announce(&me).await {
                debug!(source = source.name(), error = %e, "Announcing failed");
            }
        }

        let own = self.mesh.id();
        for source in &sources {
            let found = match source.find().await {
                Ok(found) => found,
                Err(e) => {
                    debug!(source = source.name(), error = %e, "Looking for peers failed");
                    continue;
                }
            };
            let mut state = self.state.lock().expect("discovery poisoned");
            for f in found.into_iter().filter(|f| f.addr.id != own) {
                self.mesh.remember(&f.addr);
                let entry = state.entry(f.addr.id).or_insert_with(|| Tracked {
                    known: Known {
                        addr: f.addr.clone(),
                        colony: None,
                        site: None,
                        sources: Vec::new(),
                        connected: false,
                        last_error: None,
                    },
                    next_try: Instant::now(),
                    failures: 0,
                });
                // Fresher addresses replace older ones; a source that only knows
                // the id does not blank out what another found.
                if !f.addr.direct.is_empty() || f.addr.relay.is_some() {
                    entry.known.addr = f.addr;
                }
                entry.known.colony = f.colony.or(entry.known.colony.take());
                entry.known.site = f.site.or(entry.known.site.take());
                if !entry.known.sources.contains(&source.name()) {
                    entry.known.sources.push(source.name());
                }
            }
        }

        // Dial whoever is not connected and whose retry time has come.
        let due: Vec<(NodeId, PeerAddr)> = {
            let connected: std::collections::HashSet<NodeId> =
                self.mesh.peers().into_iter().map(|p| p.id).collect();
            let mut state = self.state.lock().expect("discovery poisoned");
            let now = Instant::now();
            state
                .iter_mut()
                .filter_map(|(id, t)| {
                    t.known.connected = connected.contains(id);
                    (!t.known.connected && t.next_try <= now).then(|| (*id, t.known.addr.clone()))
                })
                .collect()
        };
        let attempts = due.into_iter().map(|(id, addr)| {
            let mesh = self.mesh.clone();
            let state = Arc::clone(&self.state);
            async move {
                let outcome = tokio::time::timeout(
                    Duration::from_secs(20),
                    mesh.connect(&addr, Protocol::Control),
                )
                .await
                .unwrap_or_else(|_| Err(NetError::Unreachable("timed out".into())));
                let mut state = state.lock().expect("discovery poisoned");
                if let Some(t) = state.get_mut(&id) {
                    match outcome {
                        Ok(_) => {
                            info!(peer = %id.fmt_short(), "Joined a peer");
                            t.known.connected = true;
                            t.known.last_error = None;
                            t.failures = 0;
                            t.next_try = Instant::now();
                        }
                        Err(e) => {
                            // Back off: 1s, 2s, 4s ... up to a minute.
                            t.failures += 1;
                            let wait = Duration::from_secs((1u64 << t.failures.min(6)).min(60));
                            t.next_try = Instant::now() + wait;
                            t.known.last_error = Some(e.to_string());
                            if t.failures == 1 {
                                warn!(peer = %id.fmt_short(), error = %e, "Could not join a peer");
                            }
                        }
                    }
                }
            }
        });
        futures_util_join_all(attempts).await;
    }
}

/// Run futures to completion together (the standard library has no `join_all`).
async fn futures_util_join_all<F: std::future::Future<Output = ()> + Send + 'static>(
    futures: impl IntoIterator<Item = F>,
) {
    let handles: Vec<_> = futures.into_iter().map(tokio::spawn).collect();
    for handle in handles {
        let _ = handle.await;
    }
}
