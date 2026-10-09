//! Behaviour: Bees, roles chosen by response thresholds, and Node temperature.
//!
//! A Node's Bees are tasks, one per core, each holding one role at a time and
//! reconsidering it between Patches. The role it takes up is its own choice,
//! drawn from the response threshold model: it engages role *j* with probability
//! `s² / (s² + θ²)` for the stimulus *s* its Node shows and its own threshold θ.
//! Thresholds differ between Bees by a seeded log-normal spread, learn from what
//! the Bee does, and are damped by a dwell time and a share-of-foragers
//! inhibitor. The Node's temperature is read from the Node itself, and a Bee
//! stops claiming work at its own cooling threshold.
//!
//! The Node plugs in through [`Duties`]: what is calling each role, and what to do
//! when a Bee answers. Dances, tremble and stop signals, quorum decisions and
//! social modes arrive in later phases, as further stimuli and roles.

mod bee;
mod colony;
mod pool;
mod roles;
mod temperature;
mod thresholds;

pub use bee::{Bee, BeeParams, Calibration, CoolingParams, Decision};
pub use colony::{BeeContext, BeeSnapshot, Colony, ColonyConfig, Duties};
pub use pool::CappedPool;
pub use roles::{Role, Stimuli};
pub use temperature::{
    FixedThermal, NoThermal, SysfsThermal, TemperatureInputs, TemperatureRegulation, ThermalSensor,
    node_temperature,
};
pub use thresholds::{ThresholdParams, Thresholds, standard_normal};
