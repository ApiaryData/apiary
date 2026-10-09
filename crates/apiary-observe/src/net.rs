//! A simulated network for the colony's transport.
//!
//! [`SimNetwork`] gives each Node a [`Transport`] (the same trait the QUIC
//! transport implements) over in-memory streams that behave like a network:
//! each hop takes time on the virtual clock, links have latency, jitter, loss
//! and bandwidth, sites can be cut apart, and a Node can sit behind a NAT.
//!
//! The NAT model follows what the Phase 2 gate measured with real NAT in
//! containers:
//!
//! - Nodes on one site reach each other directly.
//! - A Node behind a NAT reaches a public Node directly (it dials out).
//! - Anything else needs a relay. Through a plain relay the path stays relayed.
//!   Through a relay with address discovery (TLS), two Nodes behind ordinary
//!   (cone) NATs, or a public one and a cone one, punch through to a direct path
//!   a moment after connecting; a symmetric NAT cannot be punched and stays
//!   relayed.
//!
//! A relayed path costs the legs through the relay, so it is slower, and it
//! breaks if the relay goes down. Every dial, refusal, cut and path change is
//! traced; every random draw (jitter, loss) comes from the run's seed.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apiary_core::Clock;
use apiary_core::rng::{SeededRng, StdSeededRng};
use apiary_net::{Bi, Conn, NetError, NodeId, PathInfo, PathKind, PeerAddr, Protocol, Transport};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio::time::Instant;

use crate::sim::SimClock;
use crate::trace::Trace;

/// The size of each stream's buffer between a Node and the simulated wire.
const STREAM_BUFFER: usize = 256 * 1024;
/// How much a Node reads from its end of a stream at a time.
const CHUNK: usize = 16 * 1024;

/// What a Node's NAT does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Nat {
    /// A public address: anyone can send to it.
    Public,
    /// An ordinary (endpoint-independent) NAT, which can be punched through.
    Cone,
    /// A symmetric NAT: a new mapping per destination, which cannot be.
    Symmetric,
}

/// Where a Node is: its site, and what is between it and the open network.
#[derive(Clone, Debug)]
pub struct Placement {
    /// The site (a Pi site, a cloud region).
    pub site: String,
    /// Its NAT.
    pub nat: Nat,
}

impl Placement {
    /// A Node with a public address at `site`.
    pub fn public(site: &str) -> Self {
        Self::new(site, Nat::Public)
    }

    /// A Node behind an ordinary NAT at `site`.
    pub fn cone(site: &str) -> Self {
        Self::new(site, Nat::Cone)
    }

    /// A Node behind a symmetric NAT at `site`.
    pub fn symmetric(site: &str) -> Self {
        Self::new(site, Nat::Symmetric)
    }

    fn new(site: &str, nat: Nat) -> Self {
        Self {
            site: site.to_string(),
            nat,
        }
    }
}

/// What one hop of the network does to traffic.
#[derive(Clone, Copy, Debug)]
pub struct Link {
    /// The time a byte takes to cross.
    pub latency: Duration,
    /// Up to this much more, drawn from the seed.
    pub jitter: Duration,
    /// The chance that a burst is lost and resent, which costs three round trips.
    pub loss: f64,
    /// Bytes per second, if capped.
    pub bandwidth: Option<f64>,
}

impl Link {
    /// A link with this latency and nothing else wrong with it.
    pub fn with_latency(latency: Duration) -> Self {
        Self {
            latency,
            jitter: Duration::ZERO,
            loss: 0.0,
            bandwidth: None,
        }
    }

    /// Two hops in a row: the latencies and jitters add, the loss compounds, the
    /// bandwidth is the smaller.
    fn then(self, next: Link) -> Link {
        Link {
            latency: self.latency + next.latency,
            jitter: self.jitter + next.jitter,
            loss: 1.0 - (1.0 - self.loss) * (1.0 - next.loss),
            bandwidth: match (self.bandwidth, next.bandwidth) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            },
        }
    }
}

/// The relay Nodes use when they cannot reach each other directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Relay {
    /// There is none: two Nodes that cannot reach each other stay apart.
    None,
    /// A plain-HTTP relay at a site: it forwards, and paths through it stay relayed.
    Plain(String),
    /// A relay with TLS and address discovery at a site: it forwards, and lets
    /// Nodes behind ordinary NATs find a direct path.
    Tls(String),
}

struct NodeEntry {
    name: String,
    placement: Placement,
    incoming: mpsc::UnboundedSender<Arc<SimConn>>,
}

struct State {
    rng: StdSeededRng,
    nodes: HashMap<NodeId, NodeEntry>,
    links: HashMap<(String, String), Link>,
    lan: Link,
    wan: Link,
    relay: Relay,
    relay_down: bool,
    punch_delay: Duration,
    connect_timeout: Duration,
    cut: HashSet<(String, String)>,
    isolated: HashSet<NodeId>,
    conns: Vec<Arc<ConnCore>>,
}

struct Inner {
    clock: Arc<SimClock>,
    trace: Trace,
    state: Mutex<State>,
}

fn site_pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

/// How two Nodes can reach each other.
struct Plan {
    direct: Link,
    /// The path through the relay, when one is needed.
    relayed: Option<Link>,
    /// Whether the relayed path becomes a direct one after the punch delay.
    upgrades: bool,
}

/// One connection, shared by both ends.
struct ConnCore {
    dialer: NodeId,
    listener: NodeId,
    plan: Plan,
    established: Duration,
    punch_delay: Duration,
    upgrade_traced: AtomicBool,
    closed: watch::Sender<bool>,
    closed_rx: watch::Receiver<bool>,
}

impl ConnCore {
    /// The kind of path and the link it uses at virtual time `now`.
    fn current(&self, now: Duration) -> (PathKind, Link, bool) {
        match &self.plan.relayed {
            None => (PathKind::Direct, self.plan.direct, false),
            Some(relayed) => {
                if self.plan.upgrades && now >= self.established + self.punch_delay {
                    (
                        PathKind::Direct,
                        self.plan.direct,
                        !self.upgrade_traced.swap(true, Ordering::Relaxed),
                    )
                } else {
                    (PathKind::Relayed, *relayed, false)
                }
            }
        }
    }

    fn close(&self) {
        let _ = self.closed.send(true);
    }

    fn pair(&self) -> (NodeId, NodeId) {
        (self.dialer, self.listener)
    }

    fn touches(&self, node: NodeId) -> bool {
        self.dialer == node || self.listener == node
    }
}

/// A simulated network. Cheap to clone; clones are the same network.
#[derive(Clone)]
pub struct SimNetwork {
    inner: Arc<Inner>,
}

impl SimNetwork {
    pub(crate) fn new(clock: Arc<SimClock>, trace: Trace, rng: StdSeededRng) -> Self {
        Self {
            inner: Arc::new(Inner {
                clock,
                trace,
                state: Mutex::new(State {
                    rng,
                    nodes: HashMap::new(),
                    links: HashMap::new(),
                    lan: Link::with_latency(Duration::from_micros(200)),
                    wan: Link::with_latency(Duration::from_millis(12)),
                    relay: Relay::None,
                    relay_down: false,
                    punch_delay: Duration::from_secs(3),
                    connect_timeout: Duration::from_secs(5),
                    cut: HashSet::new(),
                    isolated: HashSet::new(),
                    conns: Vec::new(),
                }),
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner.state.lock().expect("network lock")
    }

    fn mark(&self, kind: &str, detail: impl Into<String>) {
        self.inner.trace.mark("net", None, kind, detail);
    }

    /// Put a Node called `name` on the network at `placement`.
    pub fn join(&self, name: &str, id: NodeId, placement: Placement) -> SimTransport {
        let (tx, rx) = mpsc::unbounded_channel();
        self.state().nodes.insert(
            id,
            NodeEntry {
                name: name.to_string(),
                placement,
                incoming: tx,
            },
        );
        SimTransport {
            id,
            net: self.clone(),
            incoming: AsyncMutex::new(rx),
            closed: AtomicBool::new(false),
            shutdown: watch::channel(false).0,
        }
    }

    /// The link between Nodes on the same site (default: 200 microseconds).
    pub fn set_lan(&self, link: Link) {
        self.state().lan = link;
    }

    /// The link between sites that have no link of their own (default: 12 ms).
    pub fn set_wan(&self, link: Link) {
        self.state().wan = link;
    }

    /// The link between two particular sites.
    pub fn set_link(&self, a: &str, b: &str, link: Link) {
        self.state().links.insert(site_pair(a, b), link);
    }

    /// Choose the relay (default: none).
    pub fn set_relay(&self, relay: Relay) {
        self.mark("net.relay", format!("{relay:?}"));
        self.state().relay = relay;
    }

    /// How long after connecting two Nodes take to punch through (default: 3 s).
    pub fn set_punch_delay(&self, delay: Duration) {
        self.state().punch_delay = delay;
    }

    /// How long a dial that cannot succeed takes to give up (default: 5 s).
    pub fn set_connect_timeout(&self, timeout: Duration) {
        self.state().connect_timeout = timeout;
    }

    /// Take the relay down or bring it back. Relayed connections close when it
    /// goes down; direct ones are unaffected.
    pub fn set_relay_down(&self, down: bool) {
        self.mark("net.relay", if down { "down" } else { "up" });
        let now = self.inner.clock.monotonic();
        let mut state = self.state();
        state.relay_down = down;
        if down {
            for conn in &state.conns {
                if matches!(conn.current(now).0, PathKind::Relayed) {
                    conn.close();
                }
            }
        }
    }

    /// Cut two sites apart: new connections between them fail and existing ones close.
    pub fn partition(&self, a: &str, b: &str) {
        self.mark("net.partition", format!("{a} | {b}"));
        let mut state = self.state();
        state.cut.insert(site_pair(a, b));
        let sites: HashMap<NodeId, String> = state
            .nodes
            .iter()
            .map(|(id, n)| (*id, n.placement.site.clone()))
            .collect();
        for conn in &state.conns {
            let (x, y) = conn.pair();
            if let (Some(sx), Some(sy)) = (sites.get(&x), sites.get(&y))
                && site_pair(sx, sy) == site_pair(a, b)
            {
                conn.close();
            }
        }
    }

    /// Restore two cut sites.
    pub fn heal(&self, a: &str, b: &str) {
        self.mark("net.heal", format!("{a} | {b}"));
        self.state().cut.remove(&site_pair(a, b));
    }

    /// Cut one Node off from everyone (its power or its cable): its connections
    /// close and nobody can dial it, until [`reconnect`](Self::reconnect).
    pub fn isolate(&self, id: NodeId) {
        let name = self.name_of(id);
        self.mark("net.isolate", name);
        let mut state = self.state();
        state.isolated.insert(id);
        for conn in &state.conns {
            if conn.touches(id) {
                conn.close();
            }
        }
    }

    /// Undo [`isolate`](Self::isolate).
    pub fn reconnect(&self, id: NodeId) {
        let name = self.name_of(id);
        self.mark("net.reconnect", name);
        self.state().isolated.remove(&id);
    }

    fn name_of(&self, id: NodeId) -> String {
        self.state()
            .nodes
            .get(&id)
            .map_or_else(|| id.fmt_short().to_string(), |n| n.name.clone())
    }

    /// How `from` reaches `to`, or why it cannot.
    fn plan(state: &State, from: NodeId, to: NodeId) -> Result<Plan, String> {
        let (Some(a), Some(b)) = (state.nodes.get(&from), state.nodes.get(&to)) else {
            return Err("no such node".into());
        };
        if state.isolated.contains(&from) || state.isolated.contains(&to) {
            return Err("a node is cut off".into());
        }
        let link = |x: &str, y: &str| {
            if x == y {
                state.lan
            } else {
                state
                    .links
                    .get(&site_pair(x, y))
                    .copied()
                    .unwrap_or(state.wan)
            }
        };
        let (sa, sb) = (&a.placement.site, &b.placement.site);
        if sa != sb && state.cut.contains(&site_pair(sa, sb)) {
            return Err(format!("{sa} and {sb} are cut apart"));
        }
        let direct = link(sa, sb);
        let (na, nb) = (a.placement.nat, b.placement.nat);
        let reaches_directly = sa == sb || nb == Nat::Public;
        if reaches_directly {
            return Ok(Plan {
                direct,
                relayed: None,
                upgrades: false,
            });
        }
        let (relay_site, tls) = match &state.relay {
            Relay::None => {
                return Err(format!(
                    "{} cannot be dialled from outside its NAT and there is no relay",
                    b.name
                ));
            }
            Relay::Plain(site) => (site, false),
            Relay::Tls(site) => (site, true),
        };
        if state.relay_down {
            return Err("the relay is down".into());
        }
        for site in [sa, sb] {
            if site != relay_site && state.cut.contains(&site_pair(site, relay_site)) {
                return Err(format!("{site} cannot reach the relay"));
            }
        }
        let relayed = link(sa, relay_site).then(link(relay_site, sb));
        let punchable = na != Nat::Symmetric && nb != Nat::Symmetric;
        Ok(Plan {
            direct,
            relayed: Some(relayed),
            upgrades: tls && punchable,
        })
    }

    /// Draw this burst's journey: how long it takes, and whether it was lost.
    fn journey(&self, core: &ConnCore, bytes: usize, free_at: &mut Duration) -> Duration {
        let now = self.inner.clock.monotonic();
        let (_, link, upgraded) = core.current(now);
        if upgraded {
            self.mark(
                "net.path",
                format!(
                    "{} <-> {} now direct",
                    self.name_of(core.dialer),
                    self.name_of(core.listener)
                ),
            );
        }
        let (jitter_draw, loss_draw) = {
            let mut state = self.state();
            (state.rng.next_f64(), state.rng.next_f64())
        };
        let mut delay = link.latency + link.jitter.mul_f64(jitter_draw);
        if loss_draw < link.loss {
            delay += link.latency * 6;
            self.mark(
                "net.loss",
                format!(
                    "{} -> {} resent after loss",
                    self.name_of(core.dialer),
                    self.name_of(core.listener)
                ),
            );
        }
        let serialise = link.bandwidth.map_or(Duration::ZERO, |bw| {
            Duration::from_secs_f64(bytes as f64 / bw)
        });
        let start = (*free_at).max(now);
        *free_at = start + serialise;
        (start - now) + serialise + delay
    }
}

/// One direction of a stream: what the sender writes comes out of the other end
/// after the network has had its way with it.
fn wire(
    net: &SimNetwork,
    core: &Arc<ConnCore>,
) -> (
    Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    Box<dyn tokio::io::AsyncRead + Send + Unpin>,
) {
    let (user_w, mut pump_r) = duplex(STREAM_BUFFER);
    let (mut pump_w, user_r) = duplex(STREAM_BUFFER);
    let (queue, mut arrivals) = mpsc::unbounded_channel::<(Instant, Option<Bytes>)>();

    let (net_in, core_in) = (net.clone(), Arc::clone(core));
    tokio::spawn(async move {
        let mut buf = vec![0u8; CHUNK];
        let mut free_at = Duration::ZERO;
        let mut last = Instant::now();
        loop {
            let n = pump_r.read(&mut buf).await.unwrap_or(0);
            let delay = net_in.journey(&core_in, n, &mut free_at);
            // Bytes stay in order even when a later burst draws a shorter journey.
            let at = (Instant::now() + delay).max(last);
            last = at;
            if n == 0 {
                let _ = queue.send((at, None));
                return;
            }
            if queue
                .send((at, Some(Bytes::copy_from_slice(&buf[..n]))))
                .is_err()
            {
                return;
            }
        }
    });

    let mut closed = core.closed_rx.clone();
    tokio::spawn(async move {
        while let Some((at, data)) = arrivals.recv().await {
            tokio::select! {
                () = tokio::time::sleep_until(at) => {}
                _ = closed.wait_for(|c| *c) => return,
            }
            match data {
                Some(bytes) => {
                    if pump_w.write_all(&bytes).await.is_err() {
                        return;
                    }
                }
                None => {
                    let _ = pump_w.shutdown().await;
                    return;
                }
            }
        }
    });

    (Box::new(user_w), Box::new(user_r))
}

fn halves(
    send: Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    recv: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
) -> Bi {
    Bi { send, recv }
}

/// One Node's end of a connection.
struct SimConn {
    core: Arc<ConnCore>,
    remote: NodeId,
    protocol: Protocol,
    net: SimNetwork,
    to_peer: mpsc::UnboundedSender<Bi>,
    from_peer: AsyncMutex<mpsc::UnboundedReceiver<Bi>>,
}

#[async_trait]
impl Conn for SimConn {
    fn remote(&self) -> NodeId {
        self.remote
    }

    fn protocol(&self) -> Protocol {
        self.protocol
    }

    async fn open_bi(&self) -> Result<Bi, NetError> {
        if *self.core.closed_rx.borrow() {
            return Err(NetError::Closed);
        }
        // The side that opens the stream is `local`; the pipes run each way.
        let (to_remote_w, to_remote_r) = wire(&self.net, &self.core);
        let (to_local_w, to_local_r) = wire(&self.net, &self.core);
        self.to_peer
            .send(halves(to_local_w, to_remote_r))
            .map_err(|_| NetError::Closed)?;
        Ok(halves(to_remote_w, to_local_r))
    }

    async fn accept_bi(&self) -> Result<Bi, NetError> {
        let mut closed = self.core.closed_rx.clone();
        let mut rx = self.from_peer.lock().await;
        loop {
            tokio::select! {
                stream = rx.recv() => return stream.ok_or(NetError::Closed),
                changed = closed.changed() => {
                    if changed.is_err() || *closed.borrow() {
                        return Err(NetError::Closed);
                    }
                }
            }
        }
    }

    fn path(&self) -> PathInfo {
        let (kind, link, upgraded) = self.core.current(self.net.inner.clock.monotonic());
        if upgraded {
            self.net.mark(
                "net.path",
                format!(
                    "{} <-> {} now direct",
                    self.net.name_of(self.core.dialer),
                    self.net.name_of(self.core.listener)
                ),
            );
        }
        PathInfo {
            kind,
            rtt: Some(link.latency * 2),
        }
    }

    fn is_closed(&self) -> bool {
        *self.core.closed_rx.borrow()
    }

    fn close(&self, _reason: &str) {
        self.core.close();
    }
}

/// One Node's end of the simulated network.
pub struct SimTransport {
    id: NodeId,
    net: SimNetwork,
    incoming: AsyncMutex<mpsc::UnboundedReceiver<Arc<SimConn>>>,
    closed: AtomicBool,
    shutdown: watch::Sender<bool>,
}

impl SimTransport {
    /// The network this Node is on.
    pub fn network(&self) -> &SimNetwork {
        &self.net
    }
}

#[async_trait]
impl Transport for SimTransport {
    fn id(&self) -> NodeId {
        self.id
    }

    async fn dial(&self, peer: &PeerAddr, protocol: Protocol) -> Result<Arc<dyn Conn>, NetError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(NetError::Closed);
        }
        let target = peer.id;
        let (me, them) = (self.net.name_of(self.id), self.net.name_of(target));
        let plan = {
            let state = self.net.state();
            SimNetwork::plan(&state, self.id, target).map(|p| (p, state.connect_timeout))
        };
        let (plan, _) = match plan {
            Ok(planned) => planned,
            Err(why) => {
                let timeout = self.net.state().connect_timeout;
                self.net.clock().sleep(timeout).await;
                self.net
                    .mark("net.dial", format!("{me} -> {them} failed: {why}"));
                return Err(NetError::Unreachable(format!(
                    "no route to {}: {why}",
                    target.fmt_short()
                )));
            }
        };
        // One round trip to set the connection up, on the path it will start on.
        let first_link = match &plan.relayed {
            Some(relayed) => *relayed,
            None => plan.direct,
        };
        self.net.clock().sleep(first_link.latency * 2).await;

        let (to_a, from_b_rx) = mpsc::unbounded_channel();
        let (to_b, from_a_rx) = mpsc::unbounded_channel();
        let (closed, closed_rx) = watch::channel(false);
        let core = {
            let mut state = self.net.state();
            // The world may have changed during the handshake.
            let still = SimNetwork::plan(&state, self.id, target);
            if let Err(why) = still {
                drop(state);
                self.net
                    .mark("net.dial", format!("{me} -> {them} failed: {why}"));
                return Err(NetError::Unreachable(format!(
                    "no route to {}: {why}",
                    target.fmt_short()
                )));
            }
            let core = Arc::new(ConnCore {
                dialer: self.id,
                listener: target,
                plan,
                established: self.net.inner.clock.monotonic(),
                punch_delay: state.punch_delay,
                upgrade_traced: AtomicBool::new(false),
                closed,
                closed_rx,
            });
            state.conns.push(Arc::clone(&core));
            core
        };
        let make = |remote, to_peer, from_peer| {
            Arc::new(SimConn {
                core: Arc::clone(&core),
                remote,
                protocol,
                net: self.net.clone(),
                to_peer,
                from_peer: AsyncMutex::new(from_peer),
            })
        };
        let mine = make(target, to_b, from_b_rx);
        let theirs = make(self.id, to_a, from_a_rx);
        let incoming = self
            .net
            .state()
            .nodes
            .get(&target)
            .map(|n| n.incoming.clone());
        let delivered = incoming.is_some_and(|tx| tx.send(theirs).is_ok());
        if !delivered {
            return Err(NetError::Unreachable(format!(
                "{} is not listening",
                target.fmt_short()
            )));
        }
        let (kind, _, _) = core.current(self.net.inner.clock.monotonic());
        self.net.mark(
            "net.dial",
            format!(
                "{me} -> {them} {protocol:?} {}",
                kind_name(kind, core.plan.upgrades)
            ),
        );
        Ok(mine)
    }

    async fn accept(&self) -> Result<Arc<dyn Conn>, NetError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(NetError::Closed);
        }
        let mut shutdown = self.shutdown.subscribe();
        let mut incoming = self.incoming.lock().await;
        tokio::select! {
            next = incoming.recv() => match next {
                Some(conn) => Ok(conn),
                None => Err(NetError::Closed),
            },
            _ = shutdown.wait_for(|stopped| *stopped) => Err(NetError::Closed),
        }
    }

    fn addr(&self) -> PeerAddr {
        PeerAddr::id_only(self.id)
    }

    async fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        let _ = self.shutdown.send(true);
        let mut state = self.net.state();
        state.nodes.remove(&self.id);
        for conn in &state.conns {
            if conn.touches(self.id) {
                conn.close();
            }
        }
    }
}

fn kind_name(kind: PathKind, upgrades: bool) -> &'static str {
    match (kind, upgrades) {
        (PathKind::Direct, _) => "direct",
        (PathKind::Relayed, true) => "relayed (will punch through)",
        (PathKind::Relayed, false) => "relayed",
        (PathKind::Unknown, _) => "unknown",
    }
}

impl SimNetwork {
    fn clock(&self) -> Arc<SimClock> {
        Arc::clone(&self.inner.clock)
    }
}

impl std::fmt::Debug for SimNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimNetwork")
    }
}
