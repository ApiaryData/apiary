//! Node temperature: how hard this Node is working, read from the Node itself.
//!
//! A honeybee colony holds its brood near 35 °C with no thermostat. Apiary drops
//! the paper's colony temperature, an aggregate that needed colony-wide data, for a
//! temperature each Node measures for itself:
//!
//! ```text
//! T = max(u_cpu, m_reserved / m_pool, q_local / q_max, τ_soc)
//! ```
//!
//! `q` is the Node's queue of Patches not yet started, and `τ_soc` is the SoC
//! temperature as a fraction of the point where the board throttles, so a hot Pi
//! cools itself by taking less work.

use std::path::PathBuf;

/// The four readings *T* is the largest of, each in `[0, 1]`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TemperatureInputs {
    /// The share of Bees busy with a Patch.
    pub cpu: f64,
    /// Memory reserved by Bees over the Node's pool.
    pub memory: f64,
    /// Patches waiting over the queue the Node tolerates.
    pub queue: f64,
    /// The SoC's heat over its throttle point.
    pub soc: f64,
}

/// Node temperature in `[0, 1]`: the largest of the four readings.
pub fn node_temperature(inputs: &TemperatureInputs) -> f64 {
    [inputs.cpu, inputs.memory, inputs.queue, inputs.soc]
        .into_iter()
        .fold(0.0, f64::max)
        .clamp(0.0, 1.0)
}

/// How a Node's temperature reads against the band it aims for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemperatureRegulation {
    /// Below 0.3: spare capacity; Bees may take up ripening and prefetching.
    Cold,
    /// 0.3 to 0.7: the band the Node aims for.
    Ideal,
    /// 0.7 to 0.85: Bees begin to stop claiming.
    Warm,
    /// 0.85 to 0.95: most Bees have stopped claiming.
    Hot,
    /// Above 0.95: the Node refuses new work.
    Critical,
}

impl TemperatureRegulation {
    /// The band `temperature` falls in.
    pub fn of(temperature: f64) -> Self {
        match temperature {
            t if t < 0.3 => Self::Cold,
            t if t <= 0.7 => Self::Ideal,
            t if t <= 0.85 => Self::Warm,
            t if t <= 0.95 => Self::Hot,
            _ => Self::Critical,
        }
    }

    /// A name for status output.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Ideal => "ideal",
            Self::Warm => "warm",
            Self::Hot => "hot",
            Self::Critical => "critical",
        }
    }
}

/// Reads how close the SoC is to throttling.
pub trait ThermalSensor: Send + Sync + 'static {
    /// The SoC temperature over its throttle point, in `[0, 1]`; zero when
    /// there is no sensor.
    fn soc_fraction(&self) -> f64;
}

/// No sensor: the SoC never limits the Node.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoThermal;

impl ThermalSensor for NoThermal {
    fn soc_fraction(&self) -> f64 {
        0.0
    }
}

/// A sensor with a value set by hand, for tests and the simulator.
#[derive(Debug, Default)]
pub struct FixedThermal(std::sync::Mutex<f64>);

impl FixedThermal {
    /// A sensor reading `fraction`.
    pub fn new(fraction: f64) -> Self {
        Self(std::sync::Mutex::new(fraction))
    }

    /// Change the reading.
    pub fn set(&self, fraction: f64) {
        *self.0.lock().expect("thermal lock") = fraction;
    }
}

impl ThermalSensor for FixedThermal {
    fn soc_fraction(&self) -> f64 {
        *self.0.lock().expect("thermal lock")
    }
}

/// Reads a Linux thermal zone (`/sys/class/thermal/thermal_zone0/temp`, in
/// millidegrees), as a fraction of the throttle point.
#[derive(Debug, Clone)]
pub struct SysfsThermal {
    path: PathBuf,
    throttle_celsius: f64,
}

impl SysfsThermal {
    /// A Raspberry Pi's SoC zone: the board begins to throttle at 80 °C.
    pub fn raspberry_pi() -> Option<Self> {
        Self::at("/sys/class/thermal/thermal_zone0/temp", 80.0)
    }

    /// The zone at `path`, if it can be read, throttling at `throttle_celsius`.
    pub fn at(path: impl Into<PathBuf>, throttle_celsius: f64) -> Option<Self> {
        let path = path.into();
        std::fs::read_to_string(&path).ok()?;
        Some(Self {
            path,
            throttle_celsius,
        })
    }
}

impl ThermalSensor for SysfsThermal {
    fn soc_fraction(&self) -> f64 {
        std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .map_or(0.0, |millis| {
                (millis / 1000.0 / self.throttle_celsius).clamp(0.0, 1.0)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temperature_is_the_largest_reading() {
        let t = node_temperature(&TemperatureInputs {
            cpu: 0.2,
            memory: 0.6,
            queue: 0.1,
            soc: 0.4,
        });
        assert_eq!(t, 0.6);
        assert_eq!(node_temperature(&TemperatureInputs::default()), 0.0);
        let over = TemperatureInputs {
            queue: 3.0,
            ..Default::default()
        };
        assert_eq!(node_temperature(&over), 1.0, "clamped");
    }

    #[test]
    fn a_hot_soc_alone_heats_the_node() {
        let soc = FixedThermal::new(0.9);
        let t = node_temperature(&TemperatureInputs {
            soc: soc.soc_fraction(),
            ..Default::default()
        });
        assert_eq!(TemperatureRegulation::of(t), TemperatureRegulation::Hot);
    }

    #[test]
    fn regulation_bands() {
        assert_eq!(TemperatureRegulation::of(0.1), TemperatureRegulation::Cold);
        assert_eq!(TemperatureRegulation::of(0.5), TemperatureRegulation::Ideal);
        assert_eq!(TemperatureRegulation::of(0.8), TemperatureRegulation::Warm);
        assert_eq!(TemperatureRegulation::of(0.9), TemperatureRegulation::Hot);
        assert_eq!(
            TemperatureRegulation::of(0.99),
            TemperatureRegulation::Critical
        );
    }

    #[test]
    fn a_sysfs_zone_reads_millidegrees_over_the_throttle_point() {
        let dir = std::env::temp_dir().join(format!("apiary-thermal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("temp");
        std::fs::write(&file, "60000\n").unwrap();
        let sensor = SysfsThermal::at(&file, 80.0).unwrap();
        assert!((sensor.soc_fraction() - 0.75).abs() < 1e-9);
        assert!(SysfsThermal::at(dir.join("missing"), 80.0).is_none());
        let _ = std::fs::remove_dir_all(dir);
    }
}
