//! Typed identifiers for Apiary entities.
//!
//! Each identifier is a newtype wrapper around `String`, providing type safety
//! so that a [`HiveId`] cannot be accidentally used where a [`FrameId`] is expected.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub String);

        impl $name {
            /// Create a new identifier from a string.
            pub fn new(id: impl Into<String>) -> Self {
                Self(id.into())
            }

            /// Generate a new random identifier using UUID v4.
            pub fn generate() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            /// Return the inner string value.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }
    };
}

define_id!(
    /// Unique identifier for a Hive (top-level namespace / database).
    HiveId
);

define_id!(
    /// Unique identifier for a Box (schema within a hive).
    BoxId
);

define_id!(
    /// Unique identifier for a Frame (table within a box).
    FrameId
);

define_id!(
    /// Unique identifier for a Cell (Parquet file in object storage).
    CellId
);

define_id!(
    /// Unique identifier for a Bee (virtual core / execution unit).
    BeeId
);

define_id!(
    /// Unique identifier for a Node (compute instance in the swarm).
    NodeId
);

define_id!(
    /// Unique identifier for a Task (unit of work submitted to the bee pool).
    TaskId
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_creation() {
        let id = HiveId::new("analytics");
        assert_eq!(id.as_str(), "analytics");
        assert_eq!(id.to_string(), "analytics");
    }

    #[test]
    fn test_id_generate() {
        let id1 = NodeId::generate();
        let id2 = NodeId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_id_equality() {
        let a = FrameId::new("temperature");
        let b = FrameId::new("temperature");
        let c = FrameId::new("humidity");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(CellId::new("cell_001"));
        set.insert(CellId::new("cell_001"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_id_from_string() {
        let id: BeeId = "bee-0".into();
        assert_eq!(id.as_str(), "bee-0");
    }

    #[test]
    fn test_id_serialize() {
        let id = BoxId::new("sensors");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"sensors\"");
        let deserialized: BoxId = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, id);
    }
}
