//! Sites: which peers are near.
//!
//! A colony is the set of Nodes with fast, cheap links to each other: one LAN, one
//! cluster network, one cloud region. Membership follows a template plus
//! measurement. The template is the site label in each Node's configuration; the
//! measurement is what each Node records about the peers it talks to: round-trip
//! time, throughput, and whether the path is direct or relayed.
//!
//! **A declared label wins.** Measurement fills in where a label is missing and
//! flags a peer whose links contradict its label (the same label across a relay,
//! or different labels on a sub-millisecond direct link), so a misconfigured Node
//! shows up instead of quietly distorting the site.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::control::{ControlService, call, decode_body};
use crate::error::NetError;
use crate::identity::NodeId;
use crate::mesh::{Admitted, Mesh};
use crate::transport::{Bi, PathKind, Protocol};

/// The control service name probes are sent to.
pub const PROBE_SERVICE: &str = "probe";

/// The most bytes one probe may ask a peer to send.
const MAX_PROBE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
struct ProbeRequest {
    bytes: u64,
}

/// Answers probes: echoes nothing, sends zeros.
pub struct ProbeService;

#[async_trait]
impl ControlService for ProbeService {
    async fn serve(
        &self,
        _peer: &Admitted,
        body: serde_json::Value,
        mut bi: Bi,
    ) -> Result<(), NetError> {
        let request: ProbeRequest = decode_body(body)?;
        let mut left = request.bytes.min(MAX_PROBE_BYTES) as usize;
        let chunk = vec![0u8; 64 * 1024];
        while left > 0 {
            let n = left.min(chunk.len());
            bi.send.write_all(&chunk[..n]).await?;
            left -= n;
        }
        bi.send.shutdown().await?;
        Ok(())
    }
}

/// What was measured about one peer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Measurement {
    /// Direct or relayed.
    pub path: PathKind,
    /// The transport's round-trip time on the selected path.
    pub rtt: Option<Duration>,
    /// Bytes per second received during a probe, if one has run.
    pub throughput: Option<f64>,
}

/// Where a peer stands relative to this Node's site.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Same site, by label or (when a label is missing) by measurement.
    SameSite,
    /// Another site.
    OtherSite,
    /// The labels and the links disagree; the declared label still decides.
    Contradiction(String),
    /// Not enough to say.
    Unknown,
}

/// What this Node thinks of a peer's site.
#[derive(Clone, Debug)]
pub struct PeerSite {
    /// The peer.
    pub id: NodeId,
    /// Its declared site label.
    pub declared: Option<String>,
    /// What was measured.
    pub measured: Measurement,
    /// The verdict.
    pub verdict: Verdict,
    /// Whether the peer counts as in this Node's site (the verdict, with a
    /// declared label deciding any contradiction).
    pub same_site: bool,
}

/// The thresholds that turn measurements into a verdict.
#[derive(Clone, Copy, Debug)]
pub struct SiteRules {
    /// A direct path at or under this round-trip time is on the LAN.
    pub near_rtt: Duration,
    /// A relayed path, or a round-trip time over this, is far.
    pub far_rtt: Duration,
}

impl Default for SiteRules {
    fn default() -> Self {
        Self {
            near_rtt: Duration::from_millis(5),
            far_rtt: Duration::from_millis(25),
        }
    }
}

/// Judge a peer from this Node's label, the peer's, and what was measured.
pub fn judge(
    mine: Option<&str>,
    theirs: Option<&str>,
    measured: &Measurement,
    rules: &SiteRules,
) -> (Verdict, bool) {
    let near =
        measured.path == PathKind::Direct && measured.rtt.is_some_and(|r| r <= rules.near_rtt);
    let far = measured.path == PathKind::Relayed || measured.rtt.is_some_and(|r| r > rules.far_rtt);

    match (mine, theirs) {
        (Some(a), Some(b)) if a == b => {
            if far {
                (
                    Verdict::Contradiction(format!(
                        "labelled '{a}' like this node but {}",
                        describe(measured)
                    )),
                    true,
                )
            } else {
                (Verdict::SameSite, true)
            }
        }
        (Some(_), Some(_)) => {
            if near {
                (
                    Verdict::Contradiction(format!(
                        "labelled differently from this node but {}",
                        describe(measured)
                    )),
                    false,
                )
            } else {
                (Verdict::OtherSite, false)
            }
        }
        // A label is missing on one side: measurement decides.
        _ => {
            if near {
                (Verdict::SameSite, true)
            } else if far {
                (Verdict::OtherSite, false)
            } else {
                (Verdict::Unknown, false)
            }
        }
    }
}

fn describe(m: &Measurement) -> String {
    match (m.path, m.rtt) {
        (PathKind::Relayed, Some(rtt)) => format!("reached through a relay ({rtt:?} round trip)"),
        (PathKind::Relayed, None) => "reached through a relay".to_string(),
        (_, Some(rtt)) => format!("{rtt:?} away on a direct link"),
        (_, None) => "no round-trip time is known".to_string(),
    }
}

/// Measures the peers a [`Mesh`] is admitted to and judges their sites.
pub struct SiteMonitor {
    mesh: Mesh,
    label: Option<String>,
    rules: SiteRules,
    measured: Mutex<HashMap<NodeId, Measurement>>,
    probe_bytes: u64,
}

impl SiteMonitor {
    /// A monitor for `mesh`, whose own site label is `label`. A probe asks a peer
    /// for `probe_bytes` bytes (0 turns throughput probing off).
    pub fn new(mesh: Mesh, label: Option<String>, rules: SiteRules, probe_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            mesh,
            label,
            rules,
            measured: Mutex::default(),
            probe_bytes,
        })
    }

    /// Measure every admitted peer once.
    pub async fn measure_all(&self) {
        for peer in self.mesh.peers() {
            let Some(conn) = self.mesh.peer_conn(peer.id, Protocol::Control) else {
                // Nothing on the control protocol yet: the transport's own view will do.
                self.measured
                    .lock()
                    .expect("site poisoned")
                    .entry(peer.id)
                    .or_insert(Measurement {
                        path: peer.path.kind,
                        rtt: peer.path.rtt,
                        throughput: None,
                    });
                continue;
            };
            let throughput = if self.probe_bytes > 0 {
                probe(&conn, self.probe_bytes).await.ok()
            } else {
                None
            };
            let path = conn.path();
            let mut measured = self.measured.lock().expect("site poisoned");
            let previous = measured.get(&peer.id).and_then(|m| m.throughput);
            measured.insert(
                peer.id,
                Measurement {
                    path: path.kind,
                    rtt: path.rtt,
                    throughput: throughput.or(previous),
                },
            );
        }
        // Forget peers that left.
        let present: std::collections::HashSet<NodeId> =
            self.mesh.peers().into_iter().map(|p| p.id).collect();
        self.measured
            .lock()
            .expect("site poisoned")
            .retain(|id, _| present.contains(id));
    }

    /// What this Node thinks of each admitted peer's site.
    pub fn view(&self) -> Vec<PeerSite> {
        let measured = self.measured.lock().expect("site poisoned");
        self.mesh
            .peers()
            .into_iter()
            .map(|peer| {
                let m = measured.get(&peer.id).copied().unwrap_or(Measurement {
                    path: peer.path.kind,
                    rtt: peer.path.rtt,
                    throughput: None,
                });
                let (verdict, same_site) =
                    judge(self.label.as_deref(), peer.site.as_deref(), &m, &self.rules);
                PeerSite {
                    id: peer.id,
                    declared: peer.site,
                    measured: m,
                    verdict,
                    same_site,
                }
            })
            .collect()
    }

    /// Measure on an interval until the returned handle is dropped or stopped.
    pub fn spawn(self: &Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        let monitor = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                monitor.measure_all().await;
                tokio::time::sleep(interval).await;
            }
        })
    }
}

/// Ask a peer for `bytes` of data and return the rate it arrived at, in bytes
/// per second.
pub async fn probe(conn: &Admitted, bytes: u64) -> Result<f64, NetError> {
    let mut bi = call(conn, PROBE_SERVICE, &ProbeRequest { bytes }).await?;
    let start = Instant::now();
    let mut received = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = bi.recv.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        received += n as u64;
    }
    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    Ok(received as f64 / elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(path: PathKind, rtt_ms: u64) -> Measurement {
        Measurement {
            path,
            rtt: Some(Duration::from_millis(rtt_ms)),
            throughput: None,
        }
    }

    fn rules() -> SiteRules {
        SiteRules::default()
    }

    #[test]
    fn a_declared_label_decides_and_measurement_agrees() {
        let near = m(PathKind::Direct, 1);
        let far = m(PathKind::Relayed, 80);
        assert_eq!(
            judge(Some("a"), Some("a"), &near, &rules()),
            (Verdict::SameSite, true)
        );
        assert_eq!(
            judge(Some("a"), Some("b"), &far, &rules()),
            (Verdict::OtherSite, false)
        );
    }

    #[test]
    fn a_label_that_the_links_contradict_is_flagged_but_still_wins() {
        let (verdict, same) = judge(Some("a"), Some("a"), &m(PathKind::Relayed, 90), &rules());
        assert!(matches!(verdict, Verdict::Contradiction(ref why) if why.contains("relay")));
        assert!(same, "the declared label decides");

        let (verdict, same) = judge(Some("a"), Some("b"), &m(PathKind::Direct, 1), &rules());
        assert!(matches!(verdict, Verdict::Contradiction(_)));
        assert!(!same);
    }

    #[test]
    fn measurement_fills_in_where_a_label_is_missing() {
        assert_eq!(
            judge(None, Some("b"), &m(PathKind::Direct, 1), &rules()),
            (Verdict::SameSite, true)
        );
        assert_eq!(
            judge(Some("a"), None, &m(PathKind::Relayed, 60), &rules()),
            (Verdict::OtherSite, false)
        );
        assert_eq!(
            judge(None, None, &m(PathKind::Direct, 12), &rules()),
            (Verdict::Unknown, false)
        );
    }

    #[test]
    fn a_direct_but_slow_link_is_far() {
        // Two sites joined by a VPN look direct to the transport but are not near.
        assert_eq!(
            judge(None, None, &m(PathKind::Direct, 60), &rules()),
            (Verdict::OtherSite, false)
        );
    }
}
