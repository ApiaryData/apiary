//! Revocation: how the Beekeeper takes a key or a token back.
//!
//! The Beekeeper keeps one cumulative list of revoked Node ids and token ids and
//! signs it with the Apiary key, numbering each version. Peers pass it to each
//! other whenever they connect and adopt any list that is newer than theirs and
//! correctly signed, so a revocation spreads without the Beekeeper reaching every
//! Node, and reaches a site that was offline when it reconnects. The list is kept
//! on disk, so a restarted Node does not forget what it was told.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use tracing::warn;

use apiary_core::{ApiaryError, Result};
use iroh_base::{PublicKey, Signature};

use crate::identity::{ApiaryKey, ApiaryPublicKey, NodeId};
use crate::token::Refusal;

/// A signed, cumulative list of what the Beekeeper has revoked.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Revocations {
    /// The version: each list the Beekeeper signs is one higher.
    pub seq: u64,
    /// When it was signed, in seconds since the epoch (the Beekeeper's clock).
    pub issued_at: i64,
    /// Revoked Node ids.
    pub nodes: BTreeSet<String>,
    /// Revoked token ids.
    pub tokens: BTreeSet<String>,
    /// The Apiary key's signature (empty only for the initial empty list).
    #[serde(default)]
    pub signature: Option<String>,
}

#[derive(Serialize)]
struct Payload<'a> {
    seq: u64,
    issued_at: i64,
    nodes: &'a BTreeSet<String>,
    tokens: &'a BTreeSet<String>,
}

impl Revocations {
    /// Nothing revoked, as every Node starts.
    pub fn empty() -> Self {
        Self::default()
    }

    fn payload(&self) -> Vec<u8> {
        serde_json::to_vec(&Payload {
            seq: self.seq,
            issued_at: self.issued_at,
            nodes: &self.nodes,
            tokens: &self.tokens,
        })
        .expect("a revocation payload always serialises")
    }

    /// Check the signature. The empty list needs none.
    pub fn verify(&self, apiary: &ApiaryPublicKey) -> std::result::Result<(), Refusal> {
        if self.seq == 0 && self.nodes.is_empty() && self.tokens.is_empty() {
            return Ok(());
        }
        let text = self.signature.as_deref().ok_or(Refusal::BadSignature)?;
        let bytes = data_encoding::BASE64URL_NOPAD
            .decode(text.as_bytes())
            .map_err(|_| Refusal::BadSignature)?;
        let signature = Signature::try_from(bytes.as_slice()).map_err(|_| Refusal::BadSignature)?;
        apiary
            .verify(&self.payload(), &signature)
            .map_err(|_| Refusal::BadSignature)
    }

    /// Whether this Node id is revoked.
    pub fn revokes_node(&self, id: &NodeId) -> bool {
        self.nodes.contains(&id.to_string())
    }

    /// Whether this token id is revoked.
    pub fn revokes_token(&self, token_id: &str) -> bool {
        self.tokens.contains(token_id)
    }
}

impl ApiaryKey {
    /// The next version of the list: everything in `current` plus these, signed.
    pub fn revoke(
        &self,
        current: &Revocations,
        nodes: &[NodeId],
        tokens: &[String],
        now: i64,
    ) -> Revocations {
        let mut next = Revocations {
            seq: current.seq + 1,
            issued_at: now,
            nodes: current.nodes.clone(),
            tokens: current.tokens.clone(),
            signature: None,
        };
        next.nodes.extend(nodes.iter().map(ToString::to_string));
        next.tokens.extend(tokens.iter().cloned());
        let signature = self.sign(&next.payload());
        next.signature = Some(data_encoding::BASE64URL_NOPAD.encode(&signature.to_bytes()));
        next
    }
}

/// A Node's copy of the revocation list, adopting newer signed ones and keeping
/// them on disk.
pub struct RevocationStore {
    path: Option<PathBuf>,
    trusted: PublicKey,
    current: RwLock<Revocations>,
}

impl RevocationStore {
    /// Open the store, reading the saved list if there is one. A saved list that
    /// does not verify is ignored with a warning.
    pub fn open(path: Option<PathBuf>, trusted: ApiaryPublicKey) -> Self {
        let current = path
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
            .and_then(|bytes| serde_json::from_slice::<Revocations>(&bytes).ok())
            .filter(|list| match list.verify(&trusted) {
                Ok(()) => true,
                Err(_) => {
                    warn!("The saved revocation list does not verify; ignoring it");
                    false
                }
            })
            .unwrap_or_default();
        Self {
            path,
            trusted,
            current: RwLock::new(current),
        }
    }

    /// The current list.
    pub fn current(&self) -> Revocations {
        self.current.read().expect("revocations poisoned").clone()
    }

    /// Whether a Node id is revoked.
    pub fn is_node_revoked(&self, id: &NodeId) -> bool {
        self.current
            .read()
            .expect("revocations poisoned")
            .revokes_node(id)
    }

    /// Offer a list. It is adopted (and saved) if it is correctly signed and
    /// newer than ours; `Ok(true)` says it was.
    pub fn offer(&self, candidate: Revocations) -> Result<bool> {
        candidate
            .verify(&self.trusted)
            .map_err(|e| ApiaryError::Config {
                message: format!("A revocation list was refused: {e}"),
            })?;
        let mut current = self.current.write().expect("revocations poisoned");
        if candidate.seq <= current.seq {
            return Ok(false);
        }
        if let Some(path) = &self.path {
            save(path, &candidate)?;
        }
        *current = candidate;
        Ok(true)
    }
}

fn save(path: &std::path::Path, list: &Revocations) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            ApiaryError::storage(format!("Failed to create {}", parent.display()), e)
        })?;
    }
    let bytes =
        serde_json::to_vec_pretty(list).map_err(|e| ApiaryError::Serialization(e.to_string()))?;
    // Written whole, then renamed into place, so a crash leaves the old list.
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)
        .and_then(|()| std::fs::rename(&tmp, path))
        .map_err(|e| ApiaryError::storage(format!("Failed to save {}", path.display()), e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeKey;

    #[test]
    fn a_signed_list_verifies_and_tampering_does_not() {
        let apiary = ApiaryKey::generate();
        let victim = NodeKey::generate().id();
        let list = apiary.revoke(&Revocations::empty(), &[victim], &[], 100);
        assert_eq!(list.seq, 1);
        assert!(list.verify(&apiary.public()).is_ok());
        assert!(list.revokes_node(&victim));

        let mut forged = list.clone();
        forged.nodes.clear();
        assert!(
            forged.verify(&apiary.public()).is_err(),
            "removing an entry breaks it"
        );

        let other = ApiaryKey::generate();
        assert!(
            list.verify(&other.public()).is_err(),
            "another key's signature is refused"
        );
    }

    #[test]
    fn each_list_is_cumulative() {
        let apiary = ApiaryKey::generate();
        let (a, b) = (NodeKey::generate().id(), NodeKey::generate().id());
        let first = apiary.revoke(&Revocations::empty(), &[a], &[], 1);
        let second = apiary.revoke(&first, &[b], &["tok".to_string()], 2);
        assert_eq!(second.seq, 2);
        assert!(second.revokes_node(&a) && second.revokes_node(&b));
        assert!(second.revokes_token("tok") && !second.revokes_token("other"));
        assert!(second.verify(&apiary.public()).is_ok());
    }

    #[test]
    fn a_store_adopts_only_newer_correctly_signed_lists() {
        let apiary = ApiaryKey::generate();
        let id = NodeKey::generate().id();
        let store = RevocationStore::open(None, apiary.public());
        assert!(!store.is_node_revoked(&id));

        let v1 = apiary.revoke(&Revocations::empty(), &[id], &[], 1);
        assert!(store.offer(v1.clone()).unwrap());
        assert!(store.is_node_revoked(&id));

        assert!(
            !store.offer(v1).unwrap(),
            "the same version is not adopted twice"
        );
        assert!(
            !store.offer(Revocations::empty()).unwrap(),
            "an older list is ignored"
        );

        let forger = ApiaryKey::generate();
        let forged = forger.revoke(&store.current(), &[], &[], 9);
        assert!(
            store.offer(forged).is_err(),
            "a list signed by another key is refused"
        );
        assert_eq!(store.current().seq, 1);
    }

    #[test]
    fn the_list_survives_a_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("state").join("revocations.json");
        let apiary = ApiaryKey::generate();
        let id = NodeKey::generate().id();

        let store = RevocationStore::open(Some(path.clone()), apiary.public());
        store
            .offer(apiary.revoke(&Revocations::empty(), &[id], &[], 5))
            .unwrap();
        drop(store);

        let reopened = RevocationStore::open(Some(path.clone()), apiary.public());
        assert!(reopened.is_node_revoked(&id));

        // A tampered file is not trusted.
        let mut saved: Revocations =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        saved.nodes.clear();
        std::fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
        let distrusted = RevocationStore::open(Some(path), apiary.public());
        assert_eq!(distrusted.current().seq, 0);
    }
}
