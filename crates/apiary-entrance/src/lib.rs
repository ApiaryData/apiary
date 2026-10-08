//! The entrance: a Flight SQL server, `DoPut` and MQTT ingest, and the Guards
//! that decide what is admitted.
//!
//! - [`Guard`] checks a deposit against its Frame's schema and either lands it
//!   in the Node's crop or refuses it.
//! - [`SetAside`] keeps what a stream's Guard refused, with the reason.

pub mod flight;
pub mod guard;
pub mod mqtt;
pub mod set_aside;

pub use flight::{FlightEntrance, RunningFlight};
pub use guard::{Admission, Guard, Source, check_batch, check_schema, fingerprint};
pub use mqtt::{MqttConfig, RunningMqtt, Subscription};
pub use set_aside::{SetAside, SetAsideRecord};
