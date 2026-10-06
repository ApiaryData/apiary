//! The dance floor: entries with versions and expiry, merged as a state-based
//! CRDT, over in-memory, gossip and comb-store transports.
//!
//! Empty until phase 5 of the redesign (the semi-social colony). Presence
//! entries will replace the V1 heartbeat files, and the V1 heartbeat-over-storage
//! logic in `apiary-runtime` becomes the `StoreFloor` fallback.
