//! The comb host's drive, served over the colony's connections.
//!
//! The site's comb lives on a drive plugged into one Node, the comb host. Every
//! other Node reaches it through that host: [`DriveService`] on the host answers
//! object-store requests against the drive, and [`DriveStore`] on the others is an
//! [`ObjectStore`] that forwards each call over the control protocol. `delta-rs`
//! and the query engine on every Node then read and commit through it.
//!
//! The rule everything rests on is create-if-absent: Delta commits, Patch
//! completion records and quorum piping records are all a conditional put of a
//! file that must not exist yet. `Put` with `PutMode::Create` is forwarded as
//! such and executed by the host's own file system, which makes it atomic, so
//! whichever Node creates the next log entry first wins and the others learn
//! they lost. The site runs no extra storage software.
//!
//! A Node's token says what it may do: reads need any capability, writes need
//! `run` or `ingest`. A read-only token cannot change the comb.
//!
//! If the host is down, `DriveStore` calls fail and Nodes keep ingesting into
//! their crops; deposits succeed once a host is back (the drive can be moved to
//! another Pi, which becomes the host when it starts).

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, UpdateVersion, UploadPart,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::control::{ControlService, call, decode_body};
use crate::error::NetError;
use crate::mesh::{Admitted, Mesh};
use crate::token::Caps;
use crate::transport::{Bi, PeerAddr, Protocol};
use crate::wire::{read_frame, write_frame};

/// The control service name the drive answers to.
pub const DRIVE_SERVICE: &str = "drive";

/// The largest object a single put may carry.
const MAX_PUT_BYTES: u64 = 1024 * 1024 * 1024;

/// How many listing entries go in one frame.
const LIST_CHUNK: usize = 500;

/// How long a put, copy or delete may take before it is abandoned.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Serialize, Deserialize, Clone, Debug)]
enum RangeWire {
    Bounded(u64, u64),
    Offset(u64),
    Suffix(u64),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum ModeWire {
    Overwrite,
    Create,
    Update {
        e_tag: Option<String>,
        version: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
enum Request {
    Get {
        path: String,
        range: Option<RangeWire>,
        head: bool,
        if_match: Option<String>,
        if_none_match: Option<String>,
    },
    Put {
        path: String,
        mode: ModeWire,
        len: u64,
    },
    Delete {
        path: String,
    },
    List {
        prefix: Option<String>,
        /// Only objects after this one, as `list_with_offset` asks.
        offset: Option<String>,
    },
    ListDelimiter {
        prefix: Option<String>,
    },
    Copy {
        from: String,
        to: String,
        create_only: bool,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MetaWire {
    location: String,
    last_modified_ms: i64,
    size: u64,
    e_tag: Option<String>,
    version: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct GetHead {
    meta: MetaWire,
    start: u64,
    end: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct PutWire {
    e_tag: Option<String>,
    version: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ListChunk {
    items: Vec<MetaWire>,
    last: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct ListDelimited {
    prefixes: Vec<String>,
    objects: Vec<MetaWire>,
}

#[derive(Serialize, Deserialize, Debug)]
enum WireError {
    NotFound(String),
    AlreadyExists(String),
    Precondition(String),
    NotModified(String),
    NotSupported(String),
    Denied(String),
    Other(String),
}

#[derive(Serialize, Deserialize, Debug)]
enum DriveReply<T> {
    Ok(T),
    Err(WireError),
}

fn meta_wire(meta: &ObjectMeta) -> MetaWire {
    MetaWire {
        location: meta.location.to_string(),
        last_modified_ms: meta.last_modified.timestamp_millis(),
        size: meta.size,
        e_tag: meta.e_tag.clone(),
        version: meta.version.clone(),
    }
}

fn meta_from_wire(wire: MetaWire) -> object_store::Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: Path::parse(&wire.location)?,
        last_modified: DateTime::<Utc>::from_timestamp_millis(wire.last_modified_ms)
            .unwrap_or_default(),
        size: wire.size,
        e_tag: wire.e_tag,
        version: wire.version,
    })
}

fn to_wire_error(e: &object_store::Error) -> WireError {
    use object_store::Error as E;
    let text = e.to_string();
    match e {
        E::NotFound { .. } => WireError::NotFound(text),
        E::AlreadyExists { .. } => WireError::AlreadyExists(text),
        E::Precondition { .. } => WireError::Precondition(text),
        E::NotModified { .. } => WireError::NotModified(text),
        E::NotSupported { .. } | E::NotImplemented { .. } => WireError::NotSupported(text),
        E::PermissionDenied { .. } | E::Unauthenticated { .. } => WireError::Denied(text),
        _ => WireError::Other(text),
    }
}

fn from_wire_error(path: &str, e: WireError) -> object_store::Error {
    use object_store::Error as E;
    let io = |m: String| -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(std::io::Error::other(m))
    };
    let path = path.to_string();
    match e {
        WireError::NotFound(m) => E::NotFound {
            path,
            source: io(m),
        },
        WireError::AlreadyExists(m) => E::AlreadyExists {
            path,
            source: io(m),
        },
        WireError::Precondition(m) => E::Precondition {
            path,
            source: io(m),
        },
        WireError::NotModified(m) => E::NotModified {
            path,
            source: io(m),
        },
        WireError::NotSupported(m) => E::NotSupported { source: io(m) },
        WireError::Denied(m) => E::PermissionDenied {
            path,
            source: io(m),
        },
        WireError::Other(m) => generic(io(m)),
    }
}

fn generic(source: Box<dyn std::error::Error + Send + Sync>) -> object_store::Error {
    object_store::Error::Generic {
        store: "apiary-drive",
        source,
    }
}

fn net_to_store(e: NetError) -> object_store::Error {
    generic(Box::new(e))
}

/// How long the host may take to answer before the request is given up. A host
/// that has gone away without closing the connection must not hang a Node.
const REPLY_TIMEOUT: Duration = Duration::from_secs(60);

async fn read_reply<T: serde::de::DeserializeOwned>(
    bi: &mut Bi,
) -> object_store::Result<DriveReply<T>> {
    tokio::time::timeout(REPLY_TIMEOUT, read_frame(&mut bi.recv))
        .await
        .map_err(|_| {
            generic(Box::new(std::io::Error::other(
                "the drive did not answer in time",
            )))
        })?
        .map_err(net_to_store)
}

// ---------------------------------------------------------------------------
// The host: serving the drive
// ---------------------------------------------------------------------------

/// Serves a store (the comb host's drive) to the colony.
pub struct DriveService {
    store: Arc<dyn ObjectStore>,
}

impl DriveService {
    /// Serve `store`, normally a `LocalFileSystem` rooted at the comb directory.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

fn may_write(caps: Caps) -> bool {
    caps.contains(Caps::RUN) || caps.contains(Caps::INGEST)
}

async fn reply<T: Serialize>(bi: &mut Bi, value: DriveReply<T>) -> Result<(), NetError> {
    write_frame(&mut bi.send, &value).await
}

async fn fail<T: Serialize>(bi: &mut Bi, e: &object_store::Error) -> Result<(), NetError> {
    reply::<T>(bi, DriveReply::Err(to_wire_error(e))).await?;
    bi.send.shutdown().await?;
    Ok(())
}

async fn deny(bi: &mut Bi, what: &str) -> Result<(), NetError> {
    reply::<()>(
        bi,
        DriveReply::Err(WireError::Denied(format!(
            "this node's token does not allow {what}"
        ))),
    )
    .await?;
    bi.send.shutdown().await?;
    Ok(())
}

fn parse_path(text: &str) -> Result<Path, object_store::Error> {
    Path::parse(text).map_err(Into::into)
}

#[async_trait]
impl ControlService for DriveService {
    async fn serve(
        &self,
        peer: &Admitted,
        body: serde_json::Value,
        mut bi: Bi,
    ) -> Result<(), NetError> {
        let request: Request = decode_body(body)?;
        let caps = peer.membership.caps;
        if !caps.allows_read() {
            return deny(&mut bi, "reading the comb").await;
        }
        match request {
            Request::Get {
                path,
                range,
                head,
                if_match,
                if_none_match,
            } => {
                let path = match parse_path(&path) {
                    Ok(p) => p,
                    Err(e) => return fail::<GetHead>(&mut bi, &e).await,
                };
                let options = GetOptions {
                    range: range.map(|r| match r {
                        RangeWire::Bounded(a, b) => GetRange::Bounded(a..b),
                        RangeWire::Offset(a) => GetRange::Offset(a),
                        RangeWire::Suffix(n) => GetRange::Suffix(n),
                    }),
                    head,
                    if_match,
                    if_none_match,
                    ..Default::default()
                };
                match self.store.get_opts(&path, options).await {
                    Ok(result) => {
                        reply(
                            &mut bi,
                            DriveReply::Ok(GetHead {
                                meta: meta_wire(&result.meta),
                                start: result.range.start,
                                end: result.range.end,
                            }),
                        )
                        .await?;
                        if !head {
                            let mut stream = result.into_stream();
                            while let Some(chunk) = stream.next().await {
                                match chunk {
                                    Ok(bytes) => bi.send.write_all(&bytes).await?,
                                    // The header already promised the bytes: end the
                                    // stream short and let the client see the shortfall.
                                    Err(_) => break,
                                }
                            }
                        }
                        bi.send.shutdown().await?;
                        Ok(())
                    }
                    Err(e) => fail::<GetHead>(&mut bi, &e).await,
                }
            }
            Request::Put { path, mode, len } => {
                if !may_write(caps) {
                    return deny(&mut bi, "changing the comb").await;
                }
                if len > MAX_PUT_BYTES {
                    return fail::<PutWire>(
                        &mut bi,
                        &generic(Box::new(std::io::Error::other(format!(
                            "a put of {len} bytes is over the {MAX_PUT_BYTES} byte limit"
                        )))),
                    )
                    .await;
                }
                let path = match parse_path(&path) {
                    Ok(p) => p,
                    Err(e) => return fail::<PutWire>(&mut bi, &e).await,
                };
                let mut data = Vec::with_capacity(len.min(1 << 20) as usize);
                (&mut bi.recv).take(len).read_to_end(&mut data).await?;
                if (data.len() as u64) != len {
                    return Err(NetError::Protocol("the put ended early".into()));
                }
                let options = PutOptions {
                    mode: match mode {
                        ModeWire::Overwrite => PutMode::Overwrite,
                        ModeWire::Create => PutMode::Create,
                        ModeWire::Update { e_tag, version } => {
                            PutMode::Update(UpdateVersion { e_tag, version })
                        }
                    },
                    ..Default::default()
                };
                match self
                    .store
                    .put_opts(&path, PutPayload::from(data), options)
                    .await
                {
                    Ok(result) => {
                        reply(
                            &mut bi,
                            DriveReply::Ok(PutWire {
                                e_tag: result.e_tag,
                                version: result.version,
                            }),
                        )
                        .await?;
                        bi.send.shutdown().await?;
                        Ok(())
                    }
                    Err(e) => fail::<PutWire>(&mut bi, &e).await,
                }
            }
            Request::Delete { path } => {
                if !may_write(caps) {
                    return deny(&mut bi, "changing the comb").await;
                }
                let outcome = match parse_path(&path) {
                    Ok(p) => self.store.delete(&p).await,
                    Err(e) => Err(e),
                };
                match outcome {
                    Ok(()) => {
                        reply(&mut bi, DriveReply::Ok(())).await?;
                        bi.send.shutdown().await?;
                        Ok(())
                    }
                    Err(e) => fail::<()>(&mut bi, &e).await,
                }
            }
            Request::List { prefix, offset } => {
                let prefix = match prefix.as_deref().map(parse_path).transpose() {
                    Ok(p) => p,
                    Err(e) => return fail::<ListChunk>(&mut bi, &e).await,
                };
                let offset = match offset.as_deref().map(parse_path).transpose() {
                    Ok(p) => p,
                    Err(e) => return fail::<ListChunk>(&mut bi, &e).await,
                };
                let stream = match &offset {
                    Some(offset) => self.store.list_with_offset(prefix.as_ref(), offset),
                    None => self.store.list(prefix.as_ref()),
                };
                // Delta's log reader assumes a listing in key order, as S3 gives
                // one; a local file system lists in directory order. So the host
                // sorts, byte by byte as S3 does, before sending.
                let mut listed: Vec<ObjectMeta> = match stream.try_collect().await {
                    Ok(listed) => listed,
                    Err(e) => return fail::<ListChunk>(&mut bi, &e).await,
                };
                listed.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
                let mut chunks = listed.chunks(LIST_CHUNK).peekable();
                if chunks.peek().is_none() {
                    reply(
                        &mut bi,
                        DriveReply::Ok(ListChunk {
                            items: Vec::new(),
                            last: true,
                        }),
                    )
                    .await?;
                }
                while let Some(chunk) = chunks.next() {
                    let chunk = ListChunk {
                        items: chunk.iter().map(meta_wire).collect(),
                        last: chunks.peek().is_none(),
                    };
                    reply(&mut bi, DriveReply::Ok(chunk)).await?;
                }
                bi.send.shutdown().await?;
                Ok(())
            }
            Request::ListDelimiter { prefix } => {
                let prefix = match prefix.as_deref().map(parse_path).transpose() {
                    Ok(p) => p,
                    Err(e) => return fail::<ListDelimited>(&mut bi, &e).await,
                };
                match self.store.list_with_delimiter(prefix.as_ref()).await {
                    Ok(listing) => {
                        reply(
                            &mut bi,
                            DriveReply::Ok(ListDelimited {
                                prefixes: listing
                                    .common_prefixes
                                    .iter()
                                    .map(ToString::to_string)
                                    .collect(),
                                objects: listing.objects.iter().map(meta_wire).collect(),
                            }),
                        )
                        .await?;
                        bi.send.shutdown().await?;
                        Ok(())
                    }
                    Err(e) => fail::<ListDelimited>(&mut bi, &e).await,
                }
            }
            Request::Copy {
                from,
                to,
                create_only,
            } => {
                if !may_write(caps) {
                    return deny(&mut bi, "changing the comb").await;
                }
                let outcome = match (parse_path(&from), parse_path(&to)) {
                    (Ok(from), Ok(to)) => {
                        let options = CopyOptions {
                            mode: if create_only {
                                CopyMode::Create
                            } else {
                                CopyMode::Overwrite
                            },
                            ..Default::default()
                        };
                        self.store.copy_opts(&from, &to, options).await
                    }
                    (Err(e), _) | (_, Err(e)) => Err(e),
                };
                match outcome {
                    Ok(()) => {
                        reply(&mut bi, DriveReply::Ok(())).await?;
                        bi.send.shutdown().await?;
                        Ok(())
                    }
                    Err(e) => fail::<()>(&mut bi, &e).await,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The client: an object store over the colony's connections
// ---------------------------------------------------------------------------

/// An [`ObjectStore`] that forwards every call to the comb host.
#[derive(Clone)]
pub struct DriveStore {
    mesh: Mesh,
    host: PeerAddr,
}

impl DriveStore {
    /// A store reaching the drive on `host` through `mesh`.
    pub fn new(mesh: Mesh, host: PeerAddr) -> Self {
        Self { mesh, host }
    }

    async fn open(&self, request: &Request) -> object_store::Result<Bi> {
        let conn = self
            .mesh
            .connect(&self.host, Protocol::Control)
            .await
            .map_err(net_to_store)?;
        call(&conn, DRIVE_SERVICE, request)
            .await
            .map_err(net_to_store)
    }

    /// Send a request that carries no body, and read the reply header.
    async fn simple<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        request: Request,
    ) -> object_store::Result<T> {
        let mut bi = self.open(&request).await?;
        let _ = bi.send.shutdown().await;
        let reply: DriveReply<T> = read_reply(&mut bi).await?;
        match reply {
            DriveReply::Ok(value) => Ok(value),
            DriveReply::Err(e) => Err(from_wire_error(path, e)),
        }
    }

    async fn put_bytes(
        &self,
        location: &Path,
        payload: &PutPayload,
        mode: ModeWire,
    ) -> object_store::Result<PutResult> {
        let request = Request::Put {
            path: location.to_string(),
            mode,
            len: payload.content_length() as u64,
        };
        let work = async {
            let mut bi = self.open(&request).await?;
            for chunk in payload.iter() {
                bi.send
                    .write_all(chunk)
                    .await
                    .map_err(|e| net_to_store(e.into()))?;
            }
            bi.send
                .shutdown()
                .await
                .map_err(|e| net_to_store(e.into()))?;
            let reply: DriveReply<PutWire> = read_reply(&mut bi).await?;
            match reply {
                DriveReply::Ok(put) => Ok(PutResult {
                    e_tag: put.e_tag,
                    version: put.version,
                }),
                DriveReply::Err(e) => Err(from_wire_error(location.as_ref(), e)),
            }
        };
        tokio::time::timeout(REQUEST_TIMEOUT, work)
            .await
            .map_err(|_| generic(Box::new(std::io::Error::other("the put timed out"))))?
    }
}

impl fmt::Debug for DriveStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DriveStore({})", self.host.id.fmt_short())
    }
}

impl fmt::Display for DriveStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the drive on {}", self.host.id.fmt_short())
    }
}

#[async_trait]
impl ObjectStore for DriveStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let mode = match opts.mode {
            PutMode::Overwrite => ModeWire::Overwrite,
            PutMode::Create => ModeWire::Create,
            PutMode::Update(v) => ModeWire::Update {
                e_tag: v.e_tag,
                version: v.version,
            },
        };
        self.put_bytes(location, &payload, mode).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(BufferedUpload {
            store: self.clone(),
            location: location.clone(),
            parts: Vec::new(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let head = options.head;
        let request = Request::Get {
            path: location.to_string(),
            range: options.range.map(|r| match r {
                GetRange::Bounded(range) => RangeWire::Bounded(range.start, range.end),
                GetRange::Offset(a) => RangeWire::Offset(a),
                GetRange::Suffix(n) => RangeWire::Suffix(n),
            }),
            head,
            if_match: options.if_match,
            if_none_match: options.if_none_match,
        };
        let mut bi = self.open(&request).await?;
        let _ = bi.send.shutdown().await;
        let reply: DriveReply<GetHead> = read_reply(&mut bi).await?;
        let head_reply = match reply {
            DriveReply::Ok(h) => h,
            DriveReply::Err(e) => return Err(from_wire_error(location.as_ref(), e)),
        };
        let meta = meta_from_wire(head_reply.meta)?;
        let (start, end) = (head_reply.start, head_reply.end);

        let payload = if head {
            futures::stream::empty().boxed()
        } else {
            futures::stream::try_unfold(
                (bi.recv, end.saturating_sub(start)),
                |(mut recv, left)| async move {
                    if left == 0 {
                        return Ok(None);
                    }
                    let mut buf = vec![0u8; left.min(256 * 1024) as usize];
                    let n = recv
                        .read(&mut buf)
                        .await
                        .map_err(|e| net_to_store(e.into()))?;
                    if n == 0 {
                        return Err(generic(Box::new(std::io::Error::other(
                            "the drive ended the object early",
                        ))));
                    }
                    buf.truncate(n);
                    Ok(Some((Bytes::from(buf), (recv, left - n as u64))))
                },
            )
            .boxed()
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range: start..end,
            attributes: Default::default(),
        })
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
                    store
                        .simple::<()>(
                            path.as_ref(),
                            Request::Delete {
                                path: path.to_string(),
                            },
                        )
                        .await
                        .or_else(|e| match e {
                            object_store::Error::NotFound { .. } => Ok(()),
                            other => Err(other),
                        })?;
                    Ok(path)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_request(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_request(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.delimited(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.copy_request(from, to, options).await
    }
}

impl DriveStore {
    fn list_request(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let request = Request::List {
            prefix: prefix.map(ToString::to_string),
            offset: offset.map(ToString::to_string),
        };
        futures::stream::once(async move {
            let mut bi = store.open(&request).await?;
            let _ = bi.send.shutdown().await;
            Ok::<_, object_store::Error>(
                futures::stream::try_unfold(
                    (bi.recv, VecDeque::<MetaWire>::new(), false),
                    |(mut recv, mut buffered, mut done)| async move {
                        loop {
                            if let Some(wire) = buffered.pop_front() {
                                let meta = meta_from_wire(wire)?;
                                return Ok(Some((meta, (recv, buffered, done))));
                            }
                            if done {
                                return Ok(None);
                            }
                            let chunk: DriveReply<ListChunk> =
                                read_frame(&mut recv).await.map_err(net_to_store)?;
                            match chunk {
                                DriveReply::Ok(chunk) => {
                                    buffered.extend(chunk.items);
                                    done = chunk.last;
                                }
                                DriveReply::Err(e) => return Err(from_wire_error("", e)),
                            }
                        }
                    },
                )
                .boxed(),
            )
        })
        .try_flatten()
        .boxed()
    }

    async fn delimited(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let listed: ListDelimited = self
            .simple(
                prefix.map_or("", |p| p.as_ref()),
                Request::ListDelimiter {
                    prefix: prefix.map(ToString::to_string),
                },
            )
            .await?;
        Ok(ListResult {
            common_prefixes: listed
                .prefixes
                .iter()
                .map(Path::parse)
                .collect::<Result<_, _>>()?,
            objects: listed
                .objects
                .into_iter()
                .map(meta_from_wire)
                .collect::<Result<_, _>>()?,
        })
    }

    async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.simple::<()>(
                to.as_ref(),
                Request::Copy {
                    from: from.to_string(),
                    to: to.to_string(),
                    create_only: matches!(options.mode, CopyMode::Create),
                },
            ),
        )
        .await
        .map_err(|_| generic(Box::new(std::io::Error::other("the copy timed out"))))?
    }
}

/// A multipart upload that holds its parts and sends them as one put on
/// completion. Cells are at most one standard size, so this is bounded.
#[derive(Debug)]
struct BufferedUpload {
    store: DriveStore,
    location: Path,
    parts: Vec<Bytes>,
}

#[async_trait]
impl MultipartUpload for BufferedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        for chunk in data.iter() {
            self.parts.push(chunk.clone());
        }
        Box::pin(async { Ok(()) })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts = std::mem::take(&mut self.parts);
        let payload = PutPayload::from_iter(parts);
        self.store
            .put_bytes(&self.location, &payload, ModeWire::Overwrite)
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.clear();
        Ok(())
    }
}
