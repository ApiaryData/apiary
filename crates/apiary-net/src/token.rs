//! Join tokens: how a Node proves it belongs to the Apiary.
//!
//! A token is signed by the Apiary key and names the Apiary, the colony, an
//! expiry and what the Node may do. It optionally names one Node id (a token for
//! a particular Pi) and carries bootstrap peers and a relay address, so a Node
//! started from nothing but a token can find the others. Comb-store credentials
//! never travel in a token.
//!
//! Peers accept a connection only from a key that presents a valid token: the
//! entrance guard's colony-odour check, applied to Nodes. The transport has
//! already proved the peer holds the key it connected as, so a token needs no
//! second signature from the Node.
//!
//! **Clocks.** A site may be offline for days and a Pi may boot with no idea of
//! the time, so a token is never refused for expiry when the verifier's clock is
//! behind the token's issue time: the clock is plainly wrong, not the token. The
//! membership is accepted and marked `clock_suspect`. A peer with a good clock
//! still checks expiry on its own side of the connection.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::identity::{ApiaryKey, ApiaryPublicKey, NodeId};
use crate::revocation::Revocations;
use iroh_base::Signature;

const PREFIX: &str = "apiary1";

/// What a Node may do in the colony.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Caps(u8);

impl Caps {
    /// Run work (Bees): reads, and takes Patches.
    pub const RUN: Caps = Caps(1);
    /// Accept deposits at its entrance.
    pub const INGEST: Caps = Caps(2);
    /// Read only.
    pub const READ: Caps = Caps(4);
    /// Everything.
    pub const ALL: Caps = Caps(7);

    /// Whether every capability in `other` is held.
    pub fn contains(self, other: Caps) -> bool {
        self.0 & other.0 == other.0
    }

    /// Whether this Node may read: any capability includes it.
    pub fn allows_read(self) -> bool {
        self.0 != 0
    }

    /// Parse `run,ingest,read` (any order, any subset).
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut caps = 0;
        for word in text.split(',').map(str::trim).filter(|w| !w.is_empty()) {
            caps |= match word {
                "run" => 1,
                "ingest" => 2,
                "read" => 4,
                other => return Err(format!("'{other}' is not a capability (run, ingest, read)")),
            };
        }
        if caps == 0 {
            return Err("A token needs at least one capability (run, ingest, read)".into());
        }
        Ok(Self(caps))
    }

    fn names(self) -> Vec<&'static str> {
        let mut names = Vec::new();
        if self.contains(Self::RUN) {
            names.push("run");
        }
        if self.contains(Self::INGEST) {
            names.push("ingest");
        }
        if self.contains(Self::READ) {
            names.push("read");
        }
        names
    }
}

impl std::fmt::Display for Caps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.names().join(","))
    }
}

impl Serialize for Caps {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.names().serialize(s)
    }
}

impl<'de> Deserialize<'de> for Caps {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let names = Vec::<String>::deserialize(d)?;
        Caps::parse(&names.join(",")).map_err(serde::de::Error::custom)
    }
}

/// A peer a new Node can dial first.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerHint {
    /// The peer's Node id.
    pub id: String,
    /// Socket addresses to try, such as `192.168.1.10:7000`.
    #[serde(default)]
    pub addrs: Vec<String>,
}

/// The signed part of a token.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Claims {
    /// Token format version.
    pub version: u8,
    /// The Apiary's name.
    pub apiary: String,
    /// The colony this Node joins.
    pub colony: String,
    /// What the Node may do.
    pub caps: Caps,
    /// When it was issued, in seconds since the epoch.
    pub issued_at: i64,
    /// When it expires.
    pub not_after: i64,
    /// A unique id, so one token can be revoked on its own.
    pub token_id: String,
    /// The one Node id that may present it (any Node, if absent).
    #[serde(default)]
    pub node: Option<String>,
    /// Peers to dial first.
    #[serde(default)]
    pub bootstrap: Vec<PeerHint>,
    /// A relay to use, as a URL.
    #[serde(default)]
    pub relay: Option<String>,
}

/// What the Beekeeper asks for when issuing a token.
#[derive(Clone, Debug)]
pub struct TokenSpec {
    /// The Apiary's name.
    pub apiary: String,
    /// The colony.
    pub colony: String,
    /// What the Node may do.
    pub caps: Caps,
    /// How long the token lasts, in seconds.
    pub lifetime_secs: i64,
    /// Bind it to one Node.
    pub node: Option<NodeId>,
    /// Peers to dial first.
    pub bootstrap: Vec<PeerHint>,
    /// A relay to use.
    pub relay: Option<String>,
}

/// Why a token or a revocation list was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// It is not a token.
    #[error("not a valid token: {0}")]
    Malformed(String),
    /// Its signature does not verify against the Apiary key.
    #[error("the signature does not verify against the Apiary key")]
    BadSignature,
    /// It names another Apiary.
    #[error("the token is for Apiary '{got}', not '{expected}'")]
    WrongApiary {
        /// This Apiary's name.
        expected: String,
        /// The name in the token.
        got: String,
    },
    /// It has expired.
    #[error("the token expired at {not_after} (it is now {now})")]
    Expired {
        /// When it expired.
        not_after: i64,
        /// The verifier's time.
        now: i64,
    },
    /// It is bound to another Node.
    #[error("the token is bound to another node")]
    WrongNode,
    /// Its key or token id has been revoked.
    #[error("revoked: {0}")]
    Revoked(String),
}

/// The Apiary a Node belongs to and the key that vouches for membership.
#[derive(Clone, Debug)]
pub struct Trust {
    /// The Apiary's name.
    pub apiary: String,
    /// The Apiary's public key.
    pub key: ApiaryPublicKey,
}

/// What a valid token grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    /// The Apiary's name.
    pub apiary: String,
    /// The colony.
    pub colony: String,
    /// What the Node may do.
    pub caps: Caps,
    /// The token's id.
    pub token_id: String,
    /// When the token expires.
    pub not_after: i64,
    /// The verifier's clock is behind the token's issue time, so expiry was not
    /// checked (see the module notes).
    pub clock_suspect: bool,
}

/// A parsed token: its claims and the text it came as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token {
    claims: Claims,
    text: String,
}

impl ApiaryKey {
    /// Issue a signed token.
    pub fn issue(&self, spec: &TokenSpec, now: i64) -> String {
        let mut id = [0u8; 12];
        id.iter_mut().for_each(|b| *b = rand_byte());
        let claims = Claims {
            version: 1,
            apiary: spec.apiary.clone(),
            colony: spec.colony.clone(),
            caps: spec.caps,
            issued_at: now,
            not_after: now.saturating_add(spec.lifetime_secs),
            token_id: data_encoding::HEXLOWER.encode(&id),
            node: spec.node.map(|n| n.to_string()),
            bootstrap: spec.bootstrap.clone(),
            relay: spec.relay.clone(),
        };
        let body = data_encoding::BASE64URL_NOPAD
            .encode(&serde_json::to_vec(&claims).expect("claims always serialise"));
        let signed = format!("{PREFIX}.{body}");
        let signature = self.sign(signed.as_bytes());
        format!(
            "{signed}.{}",
            data_encoding::BASE64URL_NOPAD.encode(&signature.to_bytes())
        )
    }
}

fn rand_byte() -> u8 {
    // The token id only has to be unique, not secret; a fresh key's bytes are a
    // convenient source of randomness that needs no extra dependency.
    use std::cell::Cell;
    thread_local! { static POOL: Cell<([u8; 32], usize)> = const { Cell::new(([0; 32], 32)) }; }
    POOL.with(|pool| {
        let (mut bytes, mut at) = pool.get();
        if at >= bytes.len() {
            bytes = iroh_base::SecretKey::generate().to_bytes();
            at = 0;
        }
        let byte = bytes[at];
        pool.set((bytes, at + 1));
        byte
    })
}

impl Token {
    /// Parse a token's text. This checks its shape, not its signature.
    pub fn parse(text: &str) -> Result<Self, Refusal> {
        let text = text.trim();
        let mut parts = text.split('.');
        let (Some(prefix), Some(body), Some(_sig), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(Refusal::Malformed(
                "expected three dot-separated parts".into(),
            ));
        };
        if prefix != PREFIX {
            return Err(Refusal::Malformed(format!(
                "unknown token format '{prefix}'"
            )));
        }
        let json = data_encoding::BASE64URL_NOPAD
            .decode(body.as_bytes())
            .map_err(|e| Refusal::Malformed(format!("bad encoding: {e}")))?;
        let claims: Claims = serde_json::from_slice(&json)
            .map_err(|e| Refusal::Malformed(format!("bad claims: {e}")))?;
        if claims.version != 1 {
            return Err(Refusal::Malformed(format!(
                "unsupported token version {}",
                claims.version
            )));
        }
        Ok(Self {
            claims,
            text: text.to_string(),
        })
    }

    /// The claims (not yet verified: use [`verify`](Self::verify)).
    pub fn claims(&self) -> &Claims {
        &self.claims
    }

    /// The token as text.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Check the token for a peer that connected as `peer`, at time `now` (seconds
    /// since the epoch), against what has been revoked.
    pub fn verify(
        &self,
        trust: &Trust,
        peer: &NodeId,
        now: i64,
        revocations: &Revocations,
    ) -> Result<Membership, Refusal> {
        let (signed, signature) = self
            .text
            .rsplit_once('.')
            .ok_or_else(|| Refusal::Malformed("no signature".into()))?;
        let signature = data_encoding::BASE64URL_NOPAD
            .decode(signature.as_bytes())
            .ok()
            .and_then(|b| Signature::try_from(b.as_slice()).ok())
            .ok_or(Refusal::BadSignature)?;
        trust
            .key
            .verify(signed.as_bytes(), &signature)
            .map_err(|_| Refusal::BadSignature)?;

        let c = &self.claims;
        if c.apiary != trust.apiary {
            return Err(Refusal::WrongApiary {
                expected: trust.apiary.clone(),
                got: c.apiary.clone(),
            });
        }
        if let Some(bound) = &c.node
            && *bound != peer.to_string()
        {
            return Err(Refusal::WrongNode);
        }
        if revocations.revokes_node(peer) {
            return Err(Refusal::Revoked(format!("node {}", peer.fmt_short())));
        }
        if revocations.revokes_token(&c.token_id) {
            return Err(Refusal::Revoked(format!("token {}", c.token_id)));
        }

        // A clock behind the token's issue time is wrong, not the token.
        let clock_suspect = now < c.issued_at;
        if !clock_suspect && now > c.not_after {
            return Err(Refusal::Expired {
                not_after: c.not_after,
                now,
            });
        }
        Ok(Membership {
            apiary: c.apiary.clone(),
            colony: c.colony.clone(),
            caps: c.caps,
            token_id: c.token_id.clone(),
            not_after: c.not_after,
            clock_suspect,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeKey;

    fn spec() -> TokenSpec {
        TokenSpec {
            apiary: "factory".into(),
            colony: "line1".into(),
            caps: Caps::RUN,
            lifetime_secs: 1000,
            node: None,
            bootstrap: vec![],
            relay: None,
        }
    }

    fn trust(key: &ApiaryKey) -> Trust {
        Trust {
            apiary: "factory".into(),
            key: key.public(),
        }
    }

    #[test]
    fn an_issued_token_verifies_for_any_node_it_is_not_bound_to() {
        let apiary = ApiaryKey::generate();
        let text = apiary.issue(&spec(), 100);
        let token = Token::parse(&text).unwrap();
        let peer = NodeKey::generate().id();
        let m = token
            .verify(&trust(&apiary), &peer, 200, &Revocations::empty())
            .unwrap();
        assert_eq!(m.colony, "line1");
        assert!(m.caps.contains(Caps::RUN) && !m.caps.contains(Caps::INGEST));
        assert!(!m.clock_suspect);
        assert_eq!(m.not_after, 1100);
    }

    #[test]
    fn a_tampered_token_is_refused() {
        let apiary = ApiaryKey::generate();
        let text = apiary.issue(&spec(), 100);
        let mut token = Token::parse(&text).unwrap();
        // Widen the capabilities and re-encode the claims under the old signature.
        let mut claims = token.claims.clone();
        claims.caps = Caps::ALL;
        let body = data_encoding::BASE64URL_NOPAD.encode(&serde_json::to_vec(&claims).unwrap());
        let sig = text.rsplit('.').next().unwrap();
        token = Token::parse(&format!("{PREFIX}.{body}.{sig}")).unwrap();
        let err = token
            .verify(
                &trust(&apiary),
                &NodeKey::generate().id(),
                200,
                &Revocations::empty(),
            )
            .unwrap_err();
        assert_eq!(err, Refusal::BadSignature);
    }

    #[test]
    fn a_token_signed_by_another_key_or_for_another_apiary_is_refused() {
        let apiary = ApiaryKey::generate();
        let rogue = ApiaryKey::generate();
        let peer = NodeKey::generate().id();
        let forged = Token::parse(&rogue.issue(&spec(), 100)).unwrap();
        assert_eq!(
            forged.verify(&trust(&apiary), &peer, 200, &Revocations::empty()),
            Err(Refusal::BadSignature)
        );

        let mut other = spec();
        other.apiary = "elsewhere".into();
        let wrong = Token::parse(&apiary.issue(&other, 100)).unwrap();
        assert!(matches!(
            wrong.verify(&trust(&apiary), &peer, 200, &Revocations::empty()),
            Err(Refusal::WrongApiary { .. })
        ));
    }

    #[test]
    fn an_expired_token_is_refused() {
        let apiary = ApiaryKey::generate();
        let token = Token::parse(&apiary.issue(&spec(), 100)).unwrap();
        let err = token
            .verify(
                &trust(&apiary),
                &NodeKey::generate().id(),
                5000,
                &Revocations::empty(),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            Refusal::Expired {
                not_after: 1100,
                now: 5000
            }
        ));
    }

    #[test]
    fn a_clock_behind_the_token_is_not_the_tokens_fault() {
        // A Pi that booted with no network time thinks it is 1970.
        let apiary = ApiaryKey::generate();
        let token = Token::parse(&apiary.issue(&spec(), 1_800_000_000)).unwrap();
        let m = token
            .verify(
                &trust(&apiary),
                &NodeKey::generate().id(),
                5,
                &Revocations::empty(),
            )
            .unwrap();
        assert!(m.clock_suspect, "accepted, and marked");
    }

    #[test]
    fn a_bound_token_works_only_for_its_node() {
        let apiary = ApiaryKey::generate();
        let (mine, other) = (NodeKey::generate().id(), NodeKey::generate().id());
        let mut bound = spec();
        bound.node = Some(mine);
        let token = Token::parse(&apiary.issue(&bound, 100)).unwrap();
        assert!(
            token
                .verify(&trust(&apiary), &mine, 200, &Revocations::empty())
                .is_ok()
        );
        assert_eq!(
            token.verify(&trust(&apiary), &other, 200, &Revocations::empty()),
            Err(Refusal::WrongNode)
        );
    }

    #[test]
    fn a_revoked_node_or_token_is_refused() {
        let apiary = ApiaryKey::generate();
        let peer = NodeKey::generate().id();
        let token = Token::parse(&apiary.issue(&spec(), 100)).unwrap();

        let by_node = apiary.revoke(&Revocations::empty(), &[peer], &[], 150);
        assert!(matches!(
            token.verify(&trust(&apiary), &peer, 200, &by_node),
            Err(Refusal::Revoked(_))
        ));

        let by_token = apiary.revoke(
            &Revocations::empty(),
            &[],
            &[token.claims().token_id.clone()],
            150,
        );
        assert!(matches!(
            token.verify(&trust(&apiary), &peer, 200, &by_token),
            Err(Refusal::Revoked(_))
        ));
        // The same token is still fine for... nobody, but another token is.
        let fresh = Token::parse(&apiary.issue(&spec(), 100)).unwrap();
        assert!(
            fresh
                .verify(&trust(&apiary), &NodeKey::generate().id(), 200, &by_token)
                .is_ok()
        );
    }

    #[test]
    fn token_ids_are_unique() {
        let apiary = ApiaryKey::generate();
        let ids: std::collections::BTreeSet<_> = (0..200)
            .map(|_| {
                Token::parse(&apiary.issue(&spec(), 100))
                    .unwrap()
                    .claims()
                    .token_id
                    .clone()
            })
            .collect();
        assert_eq!(ids.len(), 200);
    }

    #[test]
    fn capabilities_parse_and_print() {
        let caps = Caps::parse("run, read").unwrap();
        assert!(
            caps.contains(Caps::RUN) && caps.contains(Caps::READ) && !caps.contains(Caps::INGEST)
        );
        assert_eq!(caps.to_string(), "run,read");
        assert!(Caps::parse("fly").is_err());
        assert!(Caps::parse("").is_err());
        assert!(Caps::READ.allows_read() && Caps::INGEST.allows_read());
    }

    #[test]
    fn garbage_is_malformed_not_a_panic() {
        for text in [
            "",
            "x",
            "a.b",
            "apiary1.!!!.xx",
            "other.e30.xx",
            "apiary1.e30.xx.yy",
        ] {
            assert!(
                matches!(Token::parse(text), Err(Refusal::Malformed(_))),
                "{text}"
            );
        }
    }

    #[test]
    fn bootstrap_peers_and_relay_travel_in_the_token() {
        let apiary = ApiaryKey::generate();
        let mut with = spec();
        with.bootstrap = vec![PeerHint {
            id: NodeKey::generate().id().to_string(),
            addrs: vec!["10.0.0.5:7000".into()],
        }];
        with.relay = Some("https://relay.example".into());
        let token = Token::parse(&apiary.issue(&with, 100)).unwrap();
        assert_eq!(token.claims().bootstrap, with.bootstrap);
        assert_eq!(
            token.claims().relay.as_deref(),
            Some("https://relay.example")
        );
    }
}
