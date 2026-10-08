//! Networking: node identity and membership, the transport, discovery and the
//! comb host's drive service.
//!
//! - [`identity`]: Node keys and the Apiary's signing key
//! - [`token`]: join tokens that prove a Node belongs to the Apiary
//! - [`revocation`]: the Beekeeper's signed, cumulative list of revoked keys

pub mod error;
pub mod identity;
pub mod mem;
pub mod mesh;
pub mod revocation;
pub mod token;
pub mod transport;
pub mod wire;

pub use error::NetError;
pub use identity::{ApiaryKey, ApiaryPublicKey, NodeId, NodeKey, parse_public};
pub use mem::{MemNetwork, MemTransport};
pub use mesh::{Admitted, Handler, Mesh, MeshConfig, PeerInfo};
pub use revocation::{RevocationStore, Revocations};
pub use token::{Caps, Claims, Membership, PeerHint, Refusal, Token, TokenSpec, Trust};
pub use transport::{Bi, Conn, PathInfo, PathKind, PeerAddr, Protocol, Transport};
