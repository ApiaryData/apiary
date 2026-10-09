//! The observation hive: a virtual clock, a seeded RNG, an in-memory comb store
//! with injected latency and throttling, and a simulated network, so any run
//! replays exactly from its seed.
//!
//! Seeley studied colonies in glass-walled hives with paint-marked bees. This is
//! Apiary's version: run a Node (or a colony of them) inside a [`Sim`], mark what
//! each of them does in a [`Trace`], and replay the run from its seed.
//!
//! - [`Sim::run`] builds a single-threaded runtime whose clock is virtual: sleeps
//!   cost no real time, and time advances only when every task is waiting. The
//!   same code, the same seed and the same faults give the same run.
//! - [`SimStore`] is an in-memory comb store with latency, throttling, errors,
//!   lost replies and outages, all drawn from the seed.
//! - [`SimNetwork`] is a network for the colony's transport: latency, loss, cuts,
//!   NAT, and a relay that is slow and can fail.
//! - [`Trace`] records what happened, with the Node, the Bee and the virtual time.
//!   Two runs from one seed have the same [`Trace::digest`].
//!
//! The code under test is the production code. A Node started under a [`Sim`]
//! takes the sim's [`Env`](apiary_core::Env) and a simulated comb store; nothing
//! in it knows it is being observed.

mod colony;
mod marks;
mod net;
mod sim;
mod store;
mod trace;

pub use colony::{APIARY, Colony, Member};
pub use marks::{MARK_TARGET, MarkLayer};
pub use net::{Link, Nat, Placement, Relay, SimNetwork, SimTransport};
pub use sim::{Run, Sim, SimClock, wall_origin};
pub use store::{SimStore, StoreFaults};
pub use trace::{Event, Trace};
