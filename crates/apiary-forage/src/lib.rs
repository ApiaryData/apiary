//! How a colony forages: queries cut into Patches, advertised by dances, claimed,
//! run under a Bee's share of memory and handed on.
//!
//! - [`plan`]: the shape of a query, which is all foraging needs to know of it
//! - [`exec`]: the [`Executor`] that plans and runs, so the colony's behaviour can
//!   be studied on a virtual clock with made-up costs
//! - [`profit`]: profitability, and the dance that reports it
//! - [`forage`]: a Node's Scouts, Foragers, Followers and Receivers
//! - [`real`]: the executor a Node uses, over DataFusion and the comb store
//!
//! Nothing here asks another Node for anything. What a Bee knows of the colony's
//! work is its Node's copy of the dance floor and the comb store, which settles
//! which attempt at a Patch finished first.

pub mod config;
pub mod exec;
pub mod forage;
pub mod plan;
pub mod profit;
pub mod real;

pub use config::{ForageConfig, Mode};
pub use exec::{Executor, PatchRun};
pub use forage::{Answer, Forage, ForageCounts};
pub use plan::{QueryPlan, StageInfo};
pub use real::{PlanExecutor, StagedQuery};
