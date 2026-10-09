//! An in-memory comb store that misbehaves on demand.
//!
//! `SimStore` is an [`ObjectStore`] over memory, so it keeps the guarantees Delta
//! needs (create-if-absent is atomic). In front of it sit the things a real bucket
//! does to a Node on a bad day: latency, a ceiling on requests per second,
//! transient errors, replies lost after the write landed, and outages. Every
//! random draw comes from the run's seed and every wait is on the virtual clock,
//! so a scenario replays exactly.

use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use apiary_core::Clock;
use apiary_core::rng::{SeededRng, StdSeededRng};
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use crate::sim::SimClock;
use crate::trace::Trace;

/// What the store does to requests. All zero is a perfect store.
#[derive(Clone, Debug, Default)]
pub struct StoreFaults {
    /// Every request takes at least this long.
    pub latency: Duration,
    /// Plus up to this much more, drawn from the seed.
    pub jitter: Duration,
    /// A ceiling on requests per second; requests over it wait their turn.
    pub ops_per_sec: Option<f64>,
    /// The chance, per request, of a transient error.
    pub error_rate: f64,
    /// The chance, per write, that the write lands and the reply is lost, so the
    /// caller sees an error for something that happened.
    pub lost_reply_rate: f64,
    /// The store is down: every request fails (after its latency).
    pub down: bool,
    /// Windows of virtual time (since the run began) in which the store is down.
    pub outages: Vec<Range<Duration>>,
}

struct State {
    faults: StoreFaults,
    rng: StdSeededRng,
    next_slot: Duration,
}

/// What the gate decided for one request.
struct Verdict {
    /// The request is refused with this reason.
    refuse: Option<&'static str>,
    /// For a write: it lands but the caller is told it did not.
    lose_reply: bool,
}

/// The simulated comb store. Cheap to clone; clones are the same store.
#[derive(Clone)]
pub struct SimStore {
    name: String,
    inner: Arc<InMemory>,
    clock: Arc<SimClock>,
    trace: Trace,
    state: Arc<Mutex<State>>,
}

impl SimStore {
    pub(crate) fn new(name: &str, clock: Arc<SimClock>, trace: Trace, rng: StdSeededRng) -> Self {
        Self {
            name: name.to_string(),
            inner: Arc::new(InMemory::new()),
            clock,
            trace,
            state: Arc::new(Mutex::new(State {
                faults: StoreFaults::default(),
                rng,
                next_slot: Duration::ZERO,
            })),
        }
    }

    /// Make this store reachable as `apiary-drive://sim-<name>/`.
    pub(crate) fn register(&self) {
        apiary_comb::custom_store::register_store(
            &format!("sim-{}", self.name),
            Arc::new(self.clone()),
        );
    }

    /// The store's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The faults in force now.
    pub fn faults(&self) -> StoreFaults {
        self.state.lock().expect("store lock").faults.clone()
    }

    /// Change the faults: `store.set_faults(|f| f.latency = ...)`.
    pub fn set_faults(&self, change: impl FnOnce(&mut StoreFaults)) {
        change(&mut self.state.lock().expect("store lock").faults);
        self.trace.mark(
            &self.node(),
            None,
            "store.faults",
            format!("{:?}", self.faults()),
        );
    }

    /// Take the store down, or bring it back.
    pub fn set_down(&self, down: bool) {
        self.set_faults(|f| f.down = down);
    }

    /// Schedule an outage between two points in virtual time (since the run began).
    pub fn outage(&self, window: Range<Duration>) {
        self.set_faults(|f| f.outages.push(window));
    }

    /// How many objects are in the store.
    pub async fn object_count(&self) -> usize {
        self.inner.list(None).count().await
    }

    fn node(&self) -> String {
        format!("store:{}", self.name)
    }

    /// Wait out the request's latency and decide what happens to it. Three random
    /// values are drawn every time, so changing one fault never shifts the draws
    /// the others see.
    async fn gate(&self) -> Verdict {
        let (delay, verdict) = {
            let mut s = self.state.lock().expect("store lock");
            let now = self.clock.monotonic();
            let jitter_draw = s.rng.next_f64();
            let error_draw = s.rng.next_f64();
            let lose_draw = s.rng.next_f64();
            let f = s.faults.clone();
            let mut delay = f.latency + f.jitter.mul_f64(jitter_draw);
            if let Some(rate) = f.ops_per_sec.filter(|r| *r > 0.0) {
                let slot = s.next_slot.max(now);
                s.next_slot = slot + Duration::from_secs_f64(1.0 / rate);
                delay += slot - now;
            }
            // A request from a thread the simulation does not own (Delta's kernel
            // reads the log on a private thread while the Node waits) cannot wait
            // on the virtual clock, and the Node is not running meanwhile. It
            // takes no virtual time; it is still refused, failed and traced.
            if !self.clock.on_main_thread() {
                delay = Duration::ZERO;
            }
            let down = f.down || f.outages.iter().any(|w| w.contains(&now));
            let refuse = if down {
                Some("the store is down")
            } else if error_draw < f.error_rate {
                Some("a transient error")
            } else {
                None
            };
            (
                delay,
                Verdict {
                    refuse,
                    lose_reply: lose_draw < f.lost_reply_rate,
                },
            )
        };
        if !delay.is_zero() {
            self.clock.sleep(delay).await;
        }
        verdict
    }

    fn mark(&self, op: &str, path: &str, outcome: &str) {
        self.trace.mark(
            &self.node(),
            None,
            &format!("store.{op}"),
            format!("{} {outcome}", scrub_data_file(path)),
        );
    }

    fn refused(&self, op: &str, path: &str, why: &'static str) -> object_store::Error {
        self.mark(op, path, &format!("refused: {why}"));
        object_store::Error::Generic {
            store: "SimStore",
            source: why.into(),
        }
    }

    /// Run a read-like request through the gate.
    async fn read_gate(&self, op: &str, path: &str) -> object_store::Result<()> {
        let verdict = self.gate().await;
        match verdict.refuse {
            Some(why) => Err(self.refused(op, path, why)),
            None => Ok(()),
        }
    }
}

/// Delta names each data file with a random UUID it draws itself, outside the
/// simulation's control. The name carries no meaning, so the trace records it as
/// `<id>`; everything else about the write is recorded as it happened.
fn scrub_data_file(path: &str) -> String {
    if !path.contains(".parquet") {
        return path.to_string();
    }
    let bytes = path.as_bytes();
    let is_uuid = |at: usize| {
        at + 36 <= bytes.len()
            && bytes[at..at + 36].iter().enumerate().all(|(i, b)| {
                if matches!(i, 8 | 13 | 18 | 23) {
                    *b == b'-'
                } else {
                    b.is_ascii_hexdigit()
                }
            })
    };
    let mut out = String::with_capacity(path.len());
    let mut i = 0;
    while i < bytes.len() {
        if is_uuid(i) {
            out.push_str("<id>");
            i += 36;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

impl fmt::Debug for SimStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimStore({})", self.name)
    }
}

impl fmt::Display for SimStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimStore({})", self.name)
    }
}

#[async_trait]
impl ObjectStore for SimStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let path = location.to_string();
        let verdict = self.gate().await;
        if let Some(why) = verdict.refuse {
            return Err(self.refused("put", &path, why));
        }
        let created = format!("{:?}", opts.mode);
        let result = self.inner.put_opts(location, payload, opts).await;
        match &result {
            Ok(_) => {
                if verdict.lose_reply {
                    self.mark("put", &path, &format!("{created} ok, reply lost"));
                    return Err(object_store::Error::Generic {
                        store: "SimStore",
                        source: "the reply was lost".into(),
                    });
                }
                self.mark("put", &path, &format!("{created} ok"));
            }
            Err(e) => self.mark("put", &path, &format!("{created} failed: {e}")),
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let path = location.to_string();
        self.read_gate("put_multipart", &path).await?;
        self.mark("put_multipart", &path, "started");
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let path = location.to_string();
        let op = if options.head { "head" } else { "get" };
        self.read_gate(op, &path).await?;
        let result = self.inner.get_opts(location, options).await;
        self.mark(
            op,
            &path,
            &match &result {
                Ok(r) => format!("ok {} bytes", r.meta.size),
                Err(e) => format!("failed: {e}"),
            },
        );
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let store = self.clone();
        locations
            .and_then(move |path| {
                let store = store.clone();
                async move {
                    let text = path.to_string();
                    store.read_gate("delete", &text).await?;
                    store.mark("delete", &text, "ok");
                    // Deleting what is not there is not an error here.
                    match store.inner.delete(&path).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(path),
                        Err(e) => Err(e),
                    }
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let text = prefix.as_ref().map_or(String::new(), ToString::to_string);
            store.read_gate("list", &text).await?;
            store.mark("list", &text, "ok");
            Ok::<_, object_store::Error>(store.inner.list(prefix.as_ref()))
        })
        .try_flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        let offset = offset.clone();
        futures::stream::once(async move {
            let text = prefix.as_ref().map_or(String::new(), ToString::to_string);
            store.read_gate("list", &text).await?;
            store.mark("list", &text, &format!("ok after {offset}"));
            Ok::<_, object_store::Error>(store.inner.list_with_offset(prefix.as_ref(), &offset))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let text = prefix.map_or(String::new(), ToString::to_string);
        self.read_gate("list", &text).await?;
        self.mark("list", &text, "ok (delimited)");
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let text = format!("{from} -> {to}");
        let verdict = self.gate().await;
        if let Some(why) = verdict.refuse {
            return Err(self.refused("copy", &text, why));
        }
        let result = self.inner.copy_opts(from, to, options).await;
        self.mark(
            "copy",
            &text,
            &match &result {
                Ok(()) => "ok".to_string(),
                Err(e) => format!("failed: {e}"),
            },
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use super::scrub_data_file;

    #[test]
    fn a_data_files_random_id_is_scrubbed_and_nothing_else() {
        assert_eq!(
            scrub_data_file(
                "t/part-00000-816ab91e-044e-4cfb-b868-1ba789f67513-c000.snappy.parquet"
            ),
            "t/part-00000-<id>-c000.snappy.parquet"
        );
        let log = "t/_delta_log/00000000000000000001.json";
        assert_eq!(scrub_data_file(log), log);
    }
}
