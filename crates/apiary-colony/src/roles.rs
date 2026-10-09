//! The roles a Bee can hold, and the stimulus each one answers to.

/// A Bee holds one role at a time and reconsiders it only between Patches. No Bee
/// or Node assigns another's role.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub enum Role {
    /// Runs a claimed Patch: scans, operators, hand-off. In a solitary Node, a query.
    Forager,
    /// Samples a dance and claims a Patch from it (needs a dance floor: phase 5).
    Follower,
    /// Finds work nobody advertises: surveys the comb for ripening candidates.
    Scout,
    /// Accepts handed-off batches (needs tremble signals: phase 6).
    Receiver,
    /// Sorts, deduplicates, compacts, caps and harvests Cells; deposits the crop.
    Ripener,
    /// Removes expired claims, unreferenced files and abandoned outputs.
    Undertaker,
    /// Validates deposits and queries at the entrance.
    Guard,
}

impl Role {
    /// Every role.
    pub const ALL: [Role; 7] = [
        Role::Forager,
        Role::Follower,
        Role::Scout,
        Role::Receiver,
        Role::Ripener,
        Role::Undertaker,
        Role::Guard,
    ];

    /// The role's position in per-role arrays.
    pub const fn index(self) -> usize {
        match self {
            Role::Forager => 0,
            Role::Follower => 1,
            Role::Scout => 2,
            Role::Receiver => 3,
            Role::Ripener => 4,
            Role::Undertaker => 5,
            Role::Guard => 6,
        }
    }

    /// The role's name, for status and traces.
    pub const fn name(self) -> &'static str {
        match self {
            Role::Forager => "forager",
            Role::Follower => "follower",
            Role::Scout => "scout",
            Role::Receiver => "receiver",
            Role::Ripener => "ripener",
            Role::Undertaker => "undertaker",
            Role::Guard => "guard",
        }
    }
}

/// How strongly each role's work calls, as a Node reads it from its own state.
///
/// A level is a pressure with no unit: 1 means a task of that kind is waiting.
/// `forager_share` is the Node's estimate of the share of Bees foraging, which
/// damps the forager stimulus the way ethyl oleate damps foraging in a hive.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Stimuli {
    levels: [f64; 7],
    /// The estimated share of Bees foraging, in `[0, 1]`.
    pub forager_share: f64,
}

impl Stimuli {
    /// No stimulus at all.
    pub fn none() -> Self {
        Self::default()
    }

    /// Set a role's level (negative levels are zero).
    pub fn with(mut self, role: Role, level: f64) -> Self {
        self.levels[role.index()] = level.max(0.0);
        self
    }

    /// A role's level.
    pub fn get(&self, role: Role) -> f64 {
        self.levels[role.index()]
    }

    /// Set the estimated share of Bees foraging.
    pub fn with_forager_share(mut self, share: f64) -> Self {
        self.forager_share = share.clamp(0.0, 1.0);
        self
    }

    /// The strongest level of any role.
    pub fn strongest(&self) -> f64 {
        self.levels.iter().copied().fold(0.0, f64::max)
    }
}
