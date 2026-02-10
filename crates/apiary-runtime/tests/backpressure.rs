//! Integration test: Backpressure via colony temperature.
//!
//! Heavy load → temperature rises → regulation changes.

use std::sync::Arc;
use std::time::Duration;

use apiary_core::config::NodeConfig;
use apiary_runtime::behavioral::{ColonyThermometer, TemperatureRegulation};
use apiary_runtime::BeePool;

fn test_config(cores: usize) -> (NodeConfig, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = NodeConfig::detect("local://test");
    config.cores = cores;
    config.memory_per_bee = 1024 * 1024;
    config.cache_dir = tmp.path().to_path_buf();
    (config, tmp)
}

#[tokio::test]
async fn test_temperature_rises_when_bees_busy() {
    let (config, _tmp) = test_config(2);
    let pool = Arc::new(BeePool::new(&config));
    let thermo = ColonyThermometer::default();

    // Idle temperature should be low
    let idle_temp = thermo.measure(&pool).await;
    assert!(idle_temp < 0.3, "Idle temp should be Cold, got {idle_temp}");
    assert_eq!(thermo.regulation(idle_temp), TemperatureRegulation::Cold);

    // Submit long-running tasks to keep all bees busy
    let _h1 = pool
        .submit(|| {
            std::thread::sleep(Duration::from_millis(300));
            Ok(vec![])
        })
        .await;
    let _h2 = pool
        .submit(|| {
            std::thread::sleep(Duration::from_millis(300));
            Ok(vec![])
        })
        .await;

    // Give tasks time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    let busy_temp = thermo.measure(&pool).await;
    // With all bees busy: CPU util = 1.0 → temp should be >= 0.3
    assert!(
        busy_temp > idle_temp,
        "Temperature should rise under load: idle={idle_temp}, busy={busy_temp}"
    );
    assert!(
        busy_temp >= 0.3,
        "Busy temp should be >= 0.3, got {busy_temp}"
    );
}

#[tokio::test]
async fn test_temperature_returns_to_cold_after_load() {
    let (config, _tmp) = test_config(2);
    let pool = Arc::new(BeePool::new(&config));
    let thermo = ColonyThermometer::default();

    // Submit short tasks
    let h1 = pool
        .submit(|| {
            std::thread::sleep(Duration::from_millis(50));
            Ok(vec![])
        })
        .await;
    let h2 = pool
        .submit(|| {
            std::thread::sleep(Duration::from_millis(50));
            Ok(vec![])
        })
        .await;

    // Wait for tasks to complete
    h1.await.unwrap().unwrap();
    h2.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Temperature should return to low
    let temp = thermo.measure(&pool).await;
    assert!(
        temp < 0.3,
        "Temperature should return to Cold after tasks complete, got {temp}"
    );
}

#[tokio::test]
async fn test_regulation_classification_boundaries() {
    let thermo = ColonyThermometer::default();

    // Boundary tests
    assert_eq!(thermo.regulation(0.0), TemperatureRegulation::Cold);
    assert_eq!(thermo.regulation(0.29), TemperatureRegulation::Cold);
    assert_eq!(thermo.regulation(0.3), TemperatureRegulation::Ideal);
    assert_eq!(thermo.regulation(0.7), TemperatureRegulation::Ideal);
    assert_eq!(thermo.regulation(0.71), TemperatureRegulation::Warm);
    assert_eq!(thermo.regulation(0.85), TemperatureRegulation::Warm);
    assert_eq!(thermo.regulation(0.86), TemperatureRegulation::Hot);
    assert_eq!(thermo.regulation(0.95), TemperatureRegulation::Hot);
    assert_eq!(thermo.regulation(0.96), TemperatureRegulation::Critical);
    assert_eq!(thermo.regulation(1.0), TemperatureRegulation::Critical);
}

#[tokio::test]
async fn test_queue_pressure_affects_temperature() {
    let (config, _tmp) = test_config(1); // Only 1 bee
    let pool = Arc::new(BeePool::new(&config));
    let thermo = ColonyThermometer::default();

    // Fill the single bee with a long task
    let _h1 = pool
        .submit(|| {
            std::thread::sleep(Duration::from_millis(500));
            Ok(vec![])
        })
        .await;

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Temperature with 1 busy bee (no queue)
    let temp_no_queue = thermo.measure(&pool).await;

    // Add queued tasks
    let _h2 = pool
        .submit(|| {
            std::thread::sleep(Duration::from_millis(50));
            Ok(vec![])
        })
        .await;
    let _h3 = pool
        .submit(|| {
            std::thread::sleep(Duration::from_millis(50));
            Ok(vec![])
        })
        .await;

    tokio::time::sleep(Duration::from_millis(30)).await;

    let temp_with_queue = thermo.measure(&pool).await;

    // Temperature should be higher with queued tasks (queue pressure)
    assert!(
        temp_with_queue >= temp_no_queue,
        "Queue pressure should raise temperature: no_queue={temp_no_queue}, with_queue={temp_with_queue}"
    );
}
