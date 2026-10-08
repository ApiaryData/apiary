//! The comb host's drive: an object store served over the colony's connections,
//! and a Delta table committed to by several Nodes through it.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload,
};

use apiary_comb::custom_store::{register_store, unregister_store};
use apiary_comb::{CellState, Comb};
use apiary_core::{FieldDef, FrameSchema, SystemClock};
use apiary_net::{
    ApiaryKey, Caps, ControlRouter, DRIVE_SERVICE, DriveService, DriveStore, MemNetwork, Mesh,
    MeshConfig, NodeKey, PeerAddr, Protocol, RevocationStore, Token, TokenSpec, Transport, Trust,
};

struct Site {
    apiary: ApiaryKey,
    net: MemNetwork,
}

struct Member {
    key: NodeKey,
    mesh: Mesh,
}

impl Site {
    fn new() -> Self {
        Self {
            apiary: ApiaryKey::generate(),
            net: MemNetwork::new(),
        }
    }

    fn member(&self, caps: Caps) -> Member {
        let spec = TokenSpec {
            apiary: "factory".into(),
            colony: "line1".into(),
            caps,
            lifetime_secs: 3600,
            node: None,
            bootstrap: vec![],
            relay: None,
        };
        let now = chrono::Utc::now().timestamp();
        let key = NodeKey::generate();
        let transport: Arc<dyn Transport> = Arc::new(self.net.join(key.id()));
        let mesh = Mesh::new(
            transport,
            MeshConfig {
                trust: Trust {
                    apiary: "factory".into(),
                    key: self.apiary.public(),
                },
                token: Token::parse(&self.apiary.issue(&spec, now)).unwrap(),
                site: None,
                clock: SystemClock::shared(),
            },
            Arc::new(RevocationStore::open(None, self.apiary.public())),
        );
        mesh.start();
        Member { key, mesh }
    }

    /// The comb host: a drive (a temp directory) served to the colony.
    fn host(&self, dir: &std::path::Path) -> Member {
        let host = self.member(Caps::ALL);
        let router = ControlRouter::new();
        let drive: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        router.add(DRIVE_SERVICE, Arc::new(DriveService::new(drive)));
        host.mesh.register(Protocol::Control, router);
        host
    }

    fn store_for(&self, client: &Member, host: &Member) -> DriveStore {
        DriveStore::new(client.mesh.clone(), PeerAddr::id_only(host.key.id()))
    }
}

fn p(s: &str) -> Path {
    Path::from(s)
}

#[tokio::test]
async fn objects_round_trip_through_the_host() {
    let dir = tempfile::TempDir::new().unwrap();
    let site = Site::new();
    let host = site.host(dir.path());
    let client = site.member(Caps::ALL);
    let store = site.store_for(&client, &host);

    store
        .put(&p("a/one.txt"), PutPayload::from("hello drive"))
        .await
        .unwrap();
    // It is really on the host's disk.
    assert_eq!(
        std::fs::read(dir.path().join("a/one.txt")).unwrap(),
        b"hello drive"
    );

    let got = store.get(&p("a/one.txt")).await.unwrap();
    assert_eq!(got.meta.size, 11);
    assert_eq!(got.bytes().await.unwrap(), "hello drive");

    // Ranges and heads.
    let part = store.get_range(&p("a/one.txt"), 6..11).await.unwrap();
    assert_eq!(part, "drive");
    let head = store.head(&p("a/one.txt")).await.unwrap();
    assert_eq!(head.size, 11);
    assert_eq!(head.location, p("a/one.txt"));

    // Not found is a not-found error.
    let err = store.get(&p("a/missing")).await.unwrap_err();
    assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");

    store.delete(&p("a/one.txt")).await.unwrap();
    assert!(!dir.path().join("a/one.txt").exists());
}

#[tokio::test]
async fn listing_copying_and_a_large_object_work() {
    let dir = tempfile::TempDir::new().unwrap();
    let site = Site::new();
    let host = site.host(dir.path());
    let client = site.member(Caps::ALL);
    let store = site.store_for(&client, &host);

    for name in ["t/a", "t/b", "t/sub/c", "other/d"] {
        store
            .put(&p(name), PutPayload::from(name.to_string()))
            .await
            .unwrap();
    }
    let mut all: Vec<String> = store
        .list(Some(&p("t")))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap();
    all.sort();
    assert_eq!(all, ["t/a", "t/b", "t/sub/c"]);

    let delimited = store.list_with_delimiter(Some(&p("t"))).await.unwrap();
    assert_eq!(delimited.common_prefixes, vec![p("t/sub")]);
    assert_eq!(delimited.objects.len(), 2);

    store.copy(&p("t/a"), &p("t/a-copy")).await.unwrap();
    assert_eq!(
        store
            .get(&p("t/a-copy"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "t/a"
    );
    // copy_if_not_exists refuses to overwrite.
    let err = store
        .copy_if_not_exists(&p("t/b"), &p("t/a"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, object_store::Error::AlreadyExists { .. }),
        "{err}"
    );

    // A listing longer than one frame's worth of entries.
    for i in 0..1200 {
        store
            .put(&p(&format!("many/{i:05}")), PutPayload::from("x"))
            .await
            .unwrap();
    }
    let n = store
        .list(Some(&p("many")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len();
    assert_eq!(n, 1200);

    // A 24 MB object, put in one piece and in parts.
    let big: Vec<u8> = (0..24 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    store
        .put(&p("big/blob"), PutPayload::from(big.clone()))
        .await
        .unwrap();
    let back = store
        .get(&p("big/blob"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(back.len(), big.len());
    assert!(back.iter().zip(&big).all(|(a, b)| a == b));

    let mut upload = store
        .put_multipart_opts(&p("big/parts"), PutMultipartOptions::default())
        .await
        .unwrap();
    for chunk in big.chunks(5 * 1024 * 1024) {
        upload
            .put_part(PutPayload::from(chunk.to_vec()))
            .await
            .unwrap();
    }
    upload.complete().await.unwrap();
    assert_eq!(
        store.head(&p("big/parts")).await.unwrap().size,
        big.len() as u64
    );
}

#[tokio::test]
async fn create_if_absent_is_atomic_across_nodes() {
    let dir = tempfile::TempDir::new().unwrap();
    let site = Site::new();
    let host = site.host(dir.path());
    let clients: Vec<Member> = (0..8).map(|_| site.member(Caps::ALL)).collect();

    // Eight nodes race to create the same log entry. Exactly one wins.
    let tasks: Vec<_> = clients
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let store = site.store_for(c, &host);
            tokio::spawn(async move {
                store
                    .put_opts(
                        &Path::from("_delta_log/00000000000000000001.json"),
                        PutPayload::from(format!("writer {i}")),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
            })
        })
        .collect();
    let mut wins = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => wins += 1,
            Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(other) => panic!("unexpected: {other}"),
        }
    }
    assert_eq!(wins, 1, "exactly one node created the entry");
}

#[tokio::test]
async fn a_read_only_token_cannot_change_the_comb() {
    let dir = tempfile::TempDir::new().unwrap();
    let site = Site::new();
    let host = site.host(dir.path());
    let reader = site.member(Caps::READ);
    let store = site.store_for(&reader, &host);

    let writer = site.member(Caps::RUN);
    site.store_for(&writer, &host)
        .put(&p("shared"), PutPayload::from("data"))
        .await
        .unwrap();

    assert_eq!(
        store
            .get(&p("shared"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "data"
    );
    assert_eq!(
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        1
    );

    let err = store
        .put(&p("evil"), PutPayload::from("x"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, object_store::Error::PermissionDenied { .. }),
        "{err}"
    );
    assert!(store.delete(&p("shared")).await.is_err());
    assert!(store.copy(&p("shared"), &p("copy")).await.is_err());
    assert!(!dir.path().join("evil").exists());
    assert!(dir.path().join("shared").exists());
}

fn schema() -> FrameSchema {
    FrameSchema {
        fields: vec![FieldDef {
            name: "n".into(),
            data_type: "int64".into(),
            nullable: false,
        }],
    }
}

fn batch(values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![("n", Arc::new(Int64Array::from(values)) as ArrayRef)]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_node_commits_to_one_delta_table_through_the_host() {
    // The phase 2 gate: every Node in a site commits to the drive through its host.
    let dir = tempfile::TempDir::new().unwrap();
    let site = Site::new();
    let host = site.host(dir.path());
    let nodes: Vec<(String, Member)> = (0..4)
        .map(|i| (format!("node-{i}"), site.member(Caps::ALL)))
        .collect();
    for (authority, node) in &nodes {
        register_store(authority, Arc::new(site.store_for(node, &host)));
    }

    // One node creates the Frame; all four append to it concurrently.
    let comb_of =
        |authority: &str| Comb::from_storage_uri(&format!("apiary-drive://{authority}/")).unwrap();
    comb_of("node-0")
        .create_frame_table("h", "b", "readings", &schema(), &[])
        .await
        .unwrap();

    const BATCHES: i64 = 12;
    let writers: Vec<_> = nodes
        .iter()
        .enumerate()
        .map(|(i, (authority, _))| {
            let comb = comb_of(authority);
            tokio::spawn(async move {
                for b in 0..BATCHES {
                    let table = comb
                        .open_frame_table("h", "b", "readings")
                        .await
                        .unwrap()
                        .unwrap();
                    let base = (i as i64 * BATCHES + b) * 10;
                    comb.append(
                        &table,
                        &batch((base..base + 10).collect()),
                        1 << 20,
                        CellState::Nectar,
                    )
                    .await
                    .unwrap();
                }
            })
        })
        .collect();
    for w in writers {
        w.await.unwrap();
    }

    // Every row from every node is there, exactly once, as the host's disk sees it
    // and as any node reads it.
    let comb = comb_of("node-3");
    let table = comb
        .open_frame_table("h", "b", "readings")
        .await
        .unwrap()
        .unwrap();
    let rows = comb.read(&table, None).await.unwrap().unwrap();
    let mut values: Vec<i64> = rows
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    values.sort();
    let expected: Vec<i64> = (0..4 * BATCHES * 10).collect();
    assert_eq!(values, expected, "no row lost, none repeated");

    // The table's log is on the host's disk, one version per commit.
    let log = std::fs::read_dir(dir.path().join("h/b/readings/_delta_log"))
        .unwrap()
        .count();
    assert!(log > (4 * BATCHES) as usize);

    for (authority, _) in &nodes {
        unregister_store(authority);
    }
}

#[tokio::test]
async fn listings_come_back_in_key_order_whatever_the_hosts_disk_does() {
    // Delta's log reader assumes a listing in key order, as S3 gives one. A local
    // file system lists in directory order, so the host sorts before sending.
    let dir = tempfile::TempDir::new().unwrap();
    let site = Site::new();
    let host = site.host(dir.path());
    let client = site.member(Caps::ALL);
    let store = site.store_for(&client, &host);

    for i in [3, 1, 4, 0, 2, 9, 7, 5, 8, 6] {
        store
            .put(&p(&format!("log/{i:020}.json")), PutPayload::from("x"))
            .await
            .unwrap();
    }
    let names = |listed: Vec<object_store::ObjectMeta>| -> Vec<String> {
        listed.into_iter().map(|m| m.location.to_string()).collect()
    };
    let all = names(store.list(Some(&p("log"))).try_collect().await.unwrap());
    let mut sorted = all.clone();
    sorted.sort();
    assert_eq!(all, sorted);
    assert_eq!(all.len(), 10);

    // `list_with_offset` is answered by the host: only what follows the offset
    // crosses the network, in order.
    let after = names(
        store
            .list_with_offset(Some(&p("log")), &p(&format!("log/{:020}.json", 6)))
            .try_collect()
            .await
            .unwrap(),
    );
    assert_eq!(after.len(), 3);
    let mut sorted_after = after.clone();
    sorted_after.sort();
    assert_eq!(after, sorted_after);
    assert!(
        after
            .iter()
            .all(|n| n.as_str() > "log/00000000000000000006.json")
    );
}
