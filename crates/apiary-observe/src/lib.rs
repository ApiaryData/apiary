//! The observation hive: a virtual clock, a seeded RNG, an in-memory comb store
//! with injected latency and throttling, and a simulated network, so any run
//! replays exactly from its seed.
//!
//! Empty until phase 3 of the redesign. It builds on `apiary_core::Env`,
//! which already carries the clock and seed through a Node.
