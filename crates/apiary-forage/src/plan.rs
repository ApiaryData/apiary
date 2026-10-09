//! What the colony needs to know about a query's shape.
//!
//! Foraging schedules Patches without looking inside them. All it needs of a
//! query is its stages: how many Patches each has, which stages feed it, and
//! whether only the Node that planned it can run it. A [`QueryPlan`] says that.
//! The real one wraps a `datafusion` plan cut into stages; the simulator's is a
//! made-up shape with made-up costs, so the colony's behaviour can be studied on
//! a virtual clock.

use std::any::Any;

/// One stage of a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageInfo {
    /// Its position; stages that feed it have lower ids.
    pub id: usize,
    /// How many Patches it has.
    pub patches: usize,
    /// The stages whose output it reads.
    pub inputs: Vec<usize>,
    /// Only the Node that planned the query can run it (it reads rows held in that
    /// Node's memory).
    pub pinned: bool,
    /// It is the result stage: its one Patch answers the caller.
    pub result: bool,
}

/// A planned query.
pub trait QueryPlan: Send + Sync + 'static {
    /// The query's id.
    fn query(&self) -> &str;

    /// Its stages, in the order they can run.
    fn stages(&self) -> &[StageInfo];

    /// For downcasting to what the [`Executor`](crate::Executor) made.
    fn as_any(&self) -> &dyn Any;

    /// The stage with `id`.
    fn stage(&self, id: usize) -> Option<&StageInfo> {
        self.stages().iter().find(|s| s.id == id)
    }

    /// The result stage.
    fn result_stage(&self) -> &StageInfo {
        self.stages()
            .iter()
            .find(|s| s.result)
            .expect("a plan has a result stage")
    }
}
