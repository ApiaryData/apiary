//! A colony of real Nodes' membership layers on a simulated network.
//!
//! Each member is a production [`Mesh`] with its own seeded key and a token the
//! run's Apiary key signed, joined to a [`SimNetwork`] at a [`Placement`]. Nothing
//! about them is stubbed: they run admission, revocation checks and the control
//! protocols exactly as they do over QUIC.

use std::sync::Arc;

use apiary_net::{
    Admitted, Caps, Mesh, MeshConfig, NetError, NodeId, NodeKey, PeerAddr, Protocol,
    RevocationStore, Token, TokenSpec, Transport, Trust,
};

use crate::net::{Placement, SimNetwork};
use crate::sim::{Sim, wall_origin};

/// The Apiary every simulated colony belongs to.
pub const APIARY: &str = "plant";

/// One member of a simulated colony.
pub struct Member {
    /// Its name in the scenario (and in the trace).
    pub name: &'static str,
    /// Its key; the id is fixed by the run's seed.
    pub key: NodeKey,
    /// Its membership layer, already started.
    pub mesh: Mesh,
}

impl Member {
    /// Its Node id.
    pub fn id(&self) -> NodeId {
        self.key.id()
    }

    /// How to find it, by id.
    pub fn addr(&self) -> PeerAddr {
        PeerAddr::id_only(self.id())
    }

    /// Connect to another member for a protocol.
    pub async fn connect_for(
        &self,
        to: &Member,
        protocol: Protocol,
    ) -> Result<Arc<Admitted>, NetError> {
        self.mesh.connect(&to.addr(), protocol).await
    }

    /// Connect to another member's control protocol.
    pub async fn connect(&self, to: &Member) -> Result<Arc<Admitted>, NetError> {
        self.connect_for(to, Protocol::Control).await
    }
}

/// A set of members on one simulated network.
pub struct Colony {
    members: Vec<Member>,
}

impl Colony {
    /// Put `members` on `net`, each at its placement, all in one colony with
    /// tokens good for a day of virtual time.
    pub fn new(sim: &Sim, net: &SimNetwork, members: &[(&'static str, Placement)]) -> Self {
        let apiary = sim.apiary_key();
        let issued = wall_origin().timestamp();
        let members = members
            .iter()
            .map(|(name, placement)| {
                let key = sim.node_key(name);
                let spec = TokenSpec {
                    apiary: APIARY.into(),
                    colony: "plant".into(),
                    caps: Caps::ALL,
                    lifetime_secs: 86_400,
                    node: None,
                    bootstrap: vec![],
                    relay: None,
                };
                let token = Token::parse(&apiary.issue(&spec, issued)).expect("a token we issued");
                let transport: Arc<dyn Transport> =
                    Arc::new(net.join(name, key.id(), placement.clone()));
                let mesh = Mesh::new(
                    transport,
                    MeshConfig {
                        trust: Trust {
                            apiary: APIARY.into(),
                            key: apiary.public(),
                        },
                        token,
                        site: Some(placement.site.clone()),
                        clock: sim.clock(),
                    },
                    Arc::new(RevocationStore::open(None, apiary.public())),
                );
                mesh.start();
                Member { name, key, mesh }
            })
            .collect();
        Self { members }
    }

    /// The member called `name`.
    pub fn get(&self, name: &str) -> &Member {
        self.members
            .iter()
            .find(|m| m.name == name)
            .unwrap_or_else(|| panic!("no member called {name}"))
    }

    /// Every member, in the order given.
    pub fn members(&self) -> &[Member] {
        &self.members
    }
}
