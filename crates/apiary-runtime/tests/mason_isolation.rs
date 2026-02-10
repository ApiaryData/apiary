//! Integration test: Mason bee isolation.
//!
//! Memory exceeded on one bee → other bees unaffected.

use std::sync::Arc;
use std::time::Duration;

use apiary_core::config::NodeConfig;
use apiary_core::error::ApiaryError;
use apiary_core::types::BeeId;
use apiary_runtime::{BeePool, MasonChamber};

fn test_config(cores: usize) -> (NodeConfig, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = NodeConfig::detect("local://test");
    config.cores = cores;
    config.memory_per_bee = 1024 * 1024; // 1 MB per bee
    config.cache_dir = tmp.path().to_path_buf();
    (config, tmp)
}

#[tokio::test]
async fn test_memory_exceeded_does_not_affect_other_bees() {
    let (config, _tmp) = test_config(2);
    let pool = BeePool::new(&config);
    let budget = config.memory_per_bee;

    // Submit a task that fails with MemoryExceeded on one bee
    let handle = pool
        .submit(move || {
            Err(ApiaryError::MemoryExceeded {
                bee_id: BeeId::new("bee-0"),
                budget,
                requested: budget + 1,
            })
        })
        .await;

    let result = handle.await.unwrap();
    assert!(result.is_err());

    // Allow the pool to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Other bee should still work fine
    let handle2 = pool.submit(|| Ok(vec![])).await;
    let result2 = handle2.await.unwrap();
    assert!(result2.is_ok());
}

#[tokio::test]
async fn test_chamber_memory_enforcement_boundary() {
    let bee_id = BeeId::new("test-bee");
    let tmp = tempfile::TempDir::new().unwrap();
    let chamber = MasonChamber::new(
        bee_id.clone(),
        1000,
        tmp.path().to_path_buf(),
        Duration::from_secs(10),
    );

    // Exact budget should succeed
    assert!(chamber.request_memory(1000).is_ok());
    assert!((chamber.utilisation() - 1.0).abs() < f64::EPSILON);

    // Any additional allocation should fail
    let err = chamber.request_memory(1);
    assert!(err.is_err());
    match err.unwrap_err() {
        ApiaryError::MemoryExceeded { .. } => {}
        other => panic!("Expected MemoryExceeded, got: {:?}", other),
    }

    // Reset and verify
    chamber.reset();
    assert!((chamber.utilisation() - 0.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_concurrent_bees_isolated() {
    let (config, _tmp) = test_config(3);
    let pool = Arc::new(BeePool::new(&config));
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = vec![];
    for i in 0..3 {
        let c = counter.clone();
        let h = pool
            .submit(move || {
                if i == 1 {
                    // Bee 1 fails
                    Err(ApiaryError::Internal {
                        message: "simulated failure".to_string(),
                    })
                } else {
                    // Bees 0 and 2 succeed
                    c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(vec![])
                }
            })
            .await;
        handles.push(h);
    }

    let mut successes = 0;
    let mut failures = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => successes += 1,
            Err(_) => failures += 1,
        }
    }

    assert_eq!(successes, 2, "Two bees should succeed");
    assert_eq!(failures, 1, "One bee should fail");

    // All bees should return to idle
    tokio::time::sleep(Duration::from_millis(50)).await;
    let status = pool.status().await;
    for s in &status {
        assert_eq!(s.state, "idle", "Bee {} should be idle", s.bee_id);
    }
}

#[tokio::test]
async fn test_one_bee_failure_others_continue() {
    let (config, _tmp) = test_config(2);
    let pool = BeePool::new(&config);

    // Submit a task that panics on one bee
    let fail_handle = pool
        .submit(|| {
            Err(ApiaryError::Internal {
                message: "simulated crash".to_string(),
            })
        })
        .await;

    let result = fail_handle.await.unwrap();
    assert!(result.is_err());

    // Allow recovery
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Other bee should process normally
    let ok_handle = pool.submit(|| Ok(vec![])).await;
    let ok_result = ok_handle.await.unwrap();
    assert!(
        ok_result.is_ok(),
        "Other bee should still work after failure"
    );
}
