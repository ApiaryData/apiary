//! The control protocol: requests and responses between Nodes.
//!
//! Every stream on a control connection starts with a framed header naming a
//! service and carrying its request; what follows on the stream is the service's
//! own business (the drive streams file bytes, a probe streams zeros). A
//! [`ControlRouter`] is the control protocol's [`Handler`]: it reads each
//! stream's header and passes the stream to the service that owns the name.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::debug;

use crate::error::NetError;
use crate::mesh::{Admitted, Handler};
use crate::transport::Bi;
use crate::wire::{read_frame, write_frame};

/// The first frame of every control stream.
#[derive(Serialize, Deserialize)]
struct Header {
    service: String,
    body: serde_json::Value,
}

/// A service reached over the control protocol.
#[async_trait]
pub trait ControlService: Send + Sync + 'static {
    /// Serve one request. `peer` is the admitted connection it arrived on (its
    /// membership says what the caller may do), `body` the request header, and
    /// `bi` the stream to continue on.
    async fn serve(&self, peer: &Admitted, body: serde_json::Value, bi: Bi)
    -> Result<(), NetError>;
}

/// Routes control streams to services by name.
#[derive(Default)]
pub struct ControlRouter {
    services: Mutex<HashMap<String, Arc<dyn ControlService>>>,
}

impl ControlRouter {
    /// An empty router.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Serve `service` under `name`.
    pub fn add(&self, name: &str, service: Arc<dyn ControlService>) {
        self.services
            .lock()
            .expect("router poisoned")
            .insert(name.to_string(), service);
    }
}

#[async_trait]
impl Handler for ControlRouter {
    async fn handle(&self, conn: Arc<Admitted>) {
        while let Ok(mut bi) = conn.accept_bi().await {
            let header: Header = match read_frame(&mut bi.recv).await {
                Ok(header) => header,
                Err(e) => {
                    debug!(error = %e, "Unreadable control request");
                    continue;
                }
            };
            let service = self
                .services
                .lock()
                .expect("router poisoned")
                .get(&header.service)
                .cloned();
            let conn = Arc::clone(&conn);
            tokio::spawn(async move {
                match service {
                    Some(service) => {
                        if let Err(e) = service.serve(&conn, header.body, bi).await {
                            debug!(service = %header.service, error = %e, "A control request failed");
                        }
                    }
                    None => {
                        let _ = write_frame(
                            &mut bi.send,
                            &Reply::<()>::Err(format!("no such service '{}'", header.service)),
                        )
                        .await;
                        let _ = bi.send.shutdown().await;
                    }
                }
            });
        }
    }
}

/// The standard reply frame: services that answer with a header use it.
#[derive(Serialize, Deserialize)]
pub enum Reply<T> {
    /// It worked.
    Ok(T),
    /// It failed; this says why.
    Err(String),
}

/// Open a stream to a peer's control service and send the request header. The
/// caller continues on the returned stream.
pub async fn call<B: Serialize>(conn: &Admitted, service: &str, body: &B) -> Result<Bi, NetError> {
    let mut bi = conn.open_bi().await?;
    let header = Header {
        service: service.to_string(),
        body: serde_json::to_value(body).map_err(|e| NetError::Protocol(e.to_string()))?,
    };
    write_frame(&mut bi.send, &header).await?;
    Ok(bi)
}

/// Decode a service's request body.
pub fn decode_body<T: serde::de::DeserializeOwned>(body: serde_json::Value) -> Result<T, NetError> {
    serde_json::from_value(body).map_err(|e| NetError::Protocol(format!("bad request: {e}")))
}
