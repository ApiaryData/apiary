//! Discovery sources and the Discoverer that dials what they find.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use apiary_core::{ApiaryError, Result as CoreResult, StorageBackend, SystemClock};
use apiary_net::{
    Announcement, ApiaryKey, Caps, Discoverer, Discovery, Dns, DnsPeer, MemNetwork, Mesh,
    MeshConfig, NodeId, NodeKey, PeerAddr, RevocationStore, Static, StoreRendezvous, Token,
    TokenSpec, Transport, Trust,
};

/// A shared in-memory store standing in for the comb.
#[derive(Default)]
struct MemStore(Mutex<BTreeMap<String, Bytes>>);

#[async_trait]
impl StorageBackend for MemStore {
    async fn put(&self, key: &str, data: Bytes) -> CoreResult<()> {
        self.0.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }
    async fn get(&self, key: &str) -> CoreResult<Bytes> {
        self.0
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| ApiaryError::NotFound {
                key: key.to_string(),
            })
    }
    async fn list(&self, prefix: &str) -> CoreResult<Vec<String>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
    async fn delete(&self, key: &str) -> CoreResult<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> CoreResult<bool> {
        let mut map = self.0.lock().unwrap();
        if map.contains_key(key) {
            return Ok(false);
        }
        map.insert(key.to_string(), data);
        Ok(true)
    }
    async fn exists(&self, key: &str) -> CoreResult<bool> {
        Ok(self.0.lock().unwrap().contains_key(key))
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

struct Site {
    apiary: ApiaryKey,
    net: MemNetwork,
}

struct Member {
    id: NodeId,
    mesh: Mesh,
}

impl Site {
    fn new() -> Self {
        Self {
            apiary: ApiaryKey::generate(),
            net: MemNetwork::new(),
        }
    }

    /// A node with its token, or an outsider if `signed_by` is another key.
    fn node_signed_by(&self, signer: &ApiaryKey) -> Member {
        let spec = TokenSpec {
            apiary: "factory".into(),
            colony: "line1".into(),
            caps: Caps::ALL,
            lifetime_secs: 3600,
            node: None,
            bootstrap: vec![],
            relay: None,
        };
        let key = NodeKey::generate();
        let transport: Arc<dyn Transport> = Arc::new(self.net.join(key.id()));
        let mesh = Mesh::new(
            transport,
            MeshConfig {
                trust: Trust {
                    apiary: "factory".into(),
                    key: self.apiary.public(),
                },
                token: Token::parse(&signer.issue(&spec, now())).unwrap(),
                site: Some("lab".into()),
                clock: SystemClock::shared(),
            },
            Arc::new(RevocationStore::open(None, self.apiary.public())),
        );
        mesh.start();
        Member { id: key.id(), mesh }
    }

    fn node(&self) -> Member {
        self.node_signed_by(&self.apiary)
    }
}

fn announcement(m: &Member) -> Announcement {
    Announcement {
        addr: m.mesh.addr(),
        colony: "line1".into(),
        site: Some("lab".into()),
    }
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..300 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for: {what}");
}

const FAST: Duration = Duration::from_millis(50);

#[tokio::test]
async fn a_node_started_with_only_a_bootstrap_peer_joins_through_it() {
    let site = Site::new();
    let (seed, newcomer) = (site.node(), site.node());
    let sources: Vec<Arc<dyn Discovery>> =
        vec![Arc::new(Static::new(vec![PeerAddr::id_only(seed.id)]))];
    let handle = Discoverer::new(
        newcomer.mesh.clone(),
        sources,
        announcement(&newcomer),
        FAST,
    )
    .spawn();

    eventually("the newcomer to join the seed", || {
        newcomer.mesh.peers().iter().any(|p| p.id == seed.id)
    })
    .await;
    let known = handle.known();
    assert_eq!(known.len(), 1);
    assert!(known[0].connected && known[0].sources == vec!["bootstrap"]);
    handle.stop().await;
}

#[tokio::test]
async fn nodes_find_each_other_through_the_comb_and_withdraw_on_leaving() {
    let site = Site::new();
    let store: Arc<dyn StorageBackend> = Arc::new(MemStore::default());
    let nodes: Vec<Member> = (0..3).map(|_| site.node()).collect();
    let handles: Vec<_> = nodes
        .iter()
        .map(|n| {
            let sources: Vec<Arc<dyn Discovery>> =
                vec![Arc::new(StoreRendezvous::new(Arc::clone(&store)))];
            Discoverer::new(n.mesh.clone(), sources, announcement(n), FAST).spawn()
        })
        .collect();

    // Each node ends up admitted to the other two, with no addresses configured.
    eventually("a full mesh of three", || {
        nodes.iter().all(|n| n.mesh.peers().len() == 2)
    })
    .await;
    assert_eq!(store.list("floor/").await.unwrap().len(), 3);

    let mut handles = handles;
    handles.remove(0).stop().await;
    let remaining = store.list("floor/").await.unwrap();
    assert_eq!(remaining.len(), 2, "a leaving node withdraws its entry");
    assert!(
        !remaining
            .iter()
            .any(|k| k.ends_with(&nodes[0].id.to_string()))
    );
    for h in handles {
        h.stop().await;
    }
}

#[tokio::test]
async fn an_outsider_in_the_store_is_dialled_refused_and_backed_off() {
    let site = Site::new();
    let store: Arc<dyn StorageBackend> = Arc::new(MemStore::default());
    let (good, other_good) = (site.node(), site.node());
    let outsider = site.node_signed_by(&ApiaryKey::generate());

    // The outsider advertises itself too: anyone can write to a store they can reach.
    StoreRendezvous::new(Arc::clone(&store))
        .announce(&announcement(&outsider))
        .await
        .unwrap();

    let mk = |m: &Member| -> apiary_net::DiscoveryHandle {
        let sources: Vec<Arc<dyn Discovery>> =
            vec![Arc::new(StoreRendezvous::new(Arc::clone(&store)))];
        Discoverer::new(m.mesh.clone(), sources, announcement(m), FAST).spawn()
    };
    let (h1, h2) = (mk(&good), mk(&other_good));

    eventually("the two members to find each other", || {
        good.mesh.peers().iter().any(|p| p.id == other_good.id)
    })
    .await;
    eventually("the outsider to be recorded as refused", || {
        h1.known()
            .iter()
            .any(|k| k.addr.id == outsider.id && k.last_error.is_some())
    })
    .await;
    assert!(
        !good.mesh.peers().iter().any(|p| p.id == outsider.id),
        "an outsider never becomes a peer"
    );
    h1.stop().await;
    h2.stop().await;
}

#[tokio::test]
async fn dns_fills_in_addresses_for_peers_known_by_id() {
    let (a, b) = (NodeKey::generate().id(), NodeKey::generate().id());
    let dns = Dns::new(vec![
        DnsPeer {
            id: a,
            host: "localhost".into(),
            port: 7000,
        },
        DnsPeer {
            id: b,
            host: "no-such-pod.invalid".into(),
            port: 7000,
        },
    ]);
    let found = dns.find().await.unwrap();
    assert_eq!(found.len(), 1, "a pod with no DNS record yet is skipped");
    assert_eq!(found[0].addr.id, a);
    assert!(
        found[0]
            .addr
            .direct
            .iter()
            .all(|s| s.port() == 7000 && s.ip().is_loopback())
    );
    assert!(!found[0].addr.direct.is_empty());
}

#[tokio::test]
async fn mdns_announcements_are_found_on_the_lan() {
    let id = NodeKey::generate().id();
    let me = Announcement {
        addr: PeerAddr {
            id,
            direct: vec!["127.0.0.1:7123".parse().unwrap()],
            relay: None,
        },
        colony: "line1".into(),
        site: Some("lab".into()),
    };
    let announcer = apiary_net::Mdns::start().unwrap();
    let browser = apiary_net::Mdns::start().unwrap();
    announcer.announce(&me).await.unwrap();

    let mut found = None;
    for _ in 0..150 {
        found = browser
            .find()
            .await
            .unwrap()
            .into_iter()
            .find(|f| f.addr.id == id);
        if found.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let found = found.expect("the announcement was found over multicast DNS");
    assert_eq!(found.colony.as_deref(), Some("line1"));
    assert_eq!(found.site.as_deref(), Some("lab"));
    assert!(found.addr.direct.iter().all(|a| a.port() == 7123));
    assert!(!found.addr.direct.is_empty());

    announcer.withdraw(&me).await.unwrap();
}
