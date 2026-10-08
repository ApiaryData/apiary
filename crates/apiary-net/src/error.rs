//! Errors from the network layer.

use apiary_core::ApiaryError;

/// What can go wrong talking to a peer.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    /// The peer could not be reached.
    #[error("cannot reach the peer: {0}")]
    Unreachable(String),
    /// The peer refused us, or we refused it, and said why.
    #[error("membership refused: {0}")]
    Refused(String),
    /// The peer spoke out of turn or sent something unreadable.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// The connection or the transport is closed.
    #[error("closed")]
    Closed,
    /// A request failed on the other side.
    #[error("the peer reported an error: {0}")]
    Remote(String),
    /// A local I/O failure.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<NetError> for ApiaryError {
    fn from(e: NetError) -> Self {
        match e {
            NetError::Refused(message)
            | NetError::Protocol(message)
            | NetError::Remote(message) => ApiaryError::Internal { message },
            other => ApiaryError::storage_msg(other.to_string()),
        }
    }
}
