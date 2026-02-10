//! Mason bee isolation and BeePool.
//!
//! Each bee (virtual core) executes tasks in a sealed chamber — an isolated
//! execution context with hard resource boundaries. The [`BeePool`] manages
//! a pool of bees and routes submitted tasks to idle bees (or queues them).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, oneshot};
use tracing::info;

use apiary_core::config::NodeConfig;
use apiary_core::error::ApiaryError;
use apiary_core::types::{BeeId, TaskId};
use apiary_core::Result;

/// State of a bee (idle or busy with a task).
#[derive(Debug, Clone, PartialEq)]
pub enum BeeState {
    /// Bee is ready to accept work.
    Idle,
    /// Bee is executing a task.
    Busy(TaskId),
}

/// A sealed execution chamber with hard memory budget and scratch isolation.
pub struct MasonChamber {
    /// The bee that owns this chamber.
    pub bee_id: BeeId,
    /// Hard memory limit in bytes.
    pub memory_budget: u64,
    /// Current memory usage in bytes.
    pub memory_used: AtomicU64,
    /// Isolated scratch directory for this bee.
    pub scratch_dir: PathBuf,
    /// Maximum execution time per task.
    pub task_timeout: Duration,
}

impl MasonChamber {
    /// Create a new chamber with the given budget and scratch directory.
    pub fn new(
        bee_id: BeeId,
        memory_budget: u64,
        scratch_dir: PathBuf,
        task_timeout: Duration,
    ) -> Self {
        Self {
            bee_id,
            memory_budget,
            memory_used: AtomicU64::new(0),
            scratch_dir,
            task_timeout,
        }
    }

    /// Request a memory allocation. Returns an error if the budget would be exceeded.
    pub fn request_memory(&self, bytes: u64) -> Result<()> {
        let current = self.memory_used.load(Ordering::Relaxed);
        if current + bytes > self.memory_budget {
            return Err(ApiaryError::MemoryExceeded {
                bee_id: self.bee_id.clone(),
                budget: self.memory_budget,
                requested: bytes,
            });
        }
        self.memory_used.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    /// Release a previously allocated memory amount.
    pub fn release_memory(&self, bytes: u64) {
        self.memory_used.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Return current memory utilisation as a fraction (0.0 to 1.0).
    pub fn utilisation(&self) -> f64 {
        self.memory_used.load(Ordering::Relaxed) as f64 / self.memory_budget as f64
    }

    /// Reset the chamber after a task completes: zero memory and clean scratch dir.
    pub fn reset(&self) {
        self.memory_used.store(0, Ordering::Relaxed);
        if self.scratch_dir.exists() {
            let _ = std::fs::remove_dir_all(&self.scratch_dir);
        }
        let _ = std::fs::create_dir_all(&self.scratch_dir);
    }
}

/// A bee — a virtual core with its own sealed chamber.
pub struct Bee {
    /// Unique identifier.
    pub id: BeeId,
    /// Current state (idle or busy).
    pub state: Mutex<BeeState>,
    /// The bee's sealed execution chamber.
    pub chamber: MasonChamber,
}

/// Public status snapshot of a bee.
#[derive(Debug, Clone)]
pub struct BeeStatus {
    pub bee_id: String,
    pub state: String,
    pub memory_used: u64,
    pub memory_budget: u64,
}

/// A task closure that can be submitted to the pool.
type TaskFn = Box<dyn FnOnce() -> std::result::Result<Vec<arrow::record_batch::RecordBatch>, ApiaryError> + Send + 'static>;

/// A queued task waiting for a free bee.
struct QueuedTask {
    task_id: TaskId,
    func: TaskFn,
    tx: oneshot::Sender<std::result::Result<Vec<arrow::record_batch::RecordBatch>, ApiaryError>>,
}

// Safety: TaskFn is Send, oneshot::Sender is Send, TaskId is Send.
unsafe impl Send for QueuedTask {}

/// Pool of bees that routes tasks to idle workers.
pub struct BeePool {
    bees: Vec<Arc<Bee>>,
    queue: Arc<Mutex<VecDeque<QueuedTask>>>,
    default_timeout: Duration,
}

/// Default task timeout: 30 seconds.
const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(30);

impl BeePool {
    /// Create a new BeePool with N bees (N = `config.cores`), each with `memory_per_bee` budget.
    pub fn new(config: &NodeConfig) -> Self {
        let mut bees = Vec::with_capacity(config.cores);
        for i in 0..config.cores {
            let bee_id = BeeId::new(format!("bee-{i}"));
            let scratch_dir = config.cache_dir.join("scratch").join(format!("bee_{i}"));
            let _ = std::fs::create_dir_all(&scratch_dir);
            let chamber = MasonChamber::new(
                bee_id.clone(),
                config.memory_per_bee,
                scratch_dir,
                DEFAULT_TASK_TIMEOUT,
            );
            bees.push(Arc::new(Bee {
                id: bee_id,
                state: Mutex::new(BeeState::Idle),
                chamber,
            }));
        }
        info!(bees = config.cores, memory_per_bee = config.memory_per_bee, "BeePool created");
        Self {
            bees,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            default_timeout: DEFAULT_TASK_TIMEOUT,
        }
    }

    /// Return the number of bees in the pool.
    pub fn bee_count(&self) -> usize {
        self.bees.len()
    }

    /// Return the status of each bee.
    pub async fn status(&self) -> Vec<BeeStatus> {
        let mut result = Vec::with_capacity(self.bees.len());
        for bee in &self.bees {
            let state = bee.state.lock().await;
            result.push(BeeStatus {
                bee_id: bee.id.to_string(),
                state: match &*state {
                    BeeState::Idle => "idle".to_string(),
                    BeeState::Busy(tid) => format!("busy({})", tid),
                },
                memory_used: bee.chamber.memory_used.load(Ordering::Relaxed),
                memory_budget: bee.chamber.memory_budget,
            });
        }
        result
    }

    /// Submit a task to the pool. If all bees are busy the task is queued.
    /// Returns the task result via the returned JoinHandle.
    pub async fn submit<F>(
        &self,
        func: F,
    ) -> tokio::task::JoinHandle<std::result::Result<Vec<arrow::record_batch::RecordBatch>, ApiaryError>>
    where
        F: FnOnce() -> std::result::Result<Vec<arrow::record_batch::RecordBatch>, ApiaryError> + Send + 'static,
    {
        let task_id = TaskId::generate();

        // Try to find an idle bee
        for bee in &self.bees {
            let mut state = bee.state.lock().await;
            if *state == BeeState::Idle {
                *state = BeeState::Busy(task_id.clone());
                drop(state);
                return self.spawn_on_bee(Arc::clone(bee), task_id, Box::new(func));
            }
        }

        // All bees busy — queue the task
        let (tx, rx) = oneshot::channel();
        {
            let mut q = self.queue.lock().await;
            q.push_back(QueuedTask {
                task_id,
                func: Box::new(func),
                tx,
            });
        }
        info!("All bees busy, task queued");

        // Return a handle that awaits the queued result
        tokio::task::spawn(async move {
            rx.await.unwrap_or_else(|_| Err(ApiaryError::Internal {
                message: "Task channel closed before result".to_string(),
            }))
        })
    }

    /// Spawn a task on a specific bee with timeout and cleanup.
    fn spawn_on_bee(
        &self,
        bee: Arc<Bee>,
        task_id: TaskId,
        func: TaskFn,
    ) -> tokio::task::JoinHandle<std::result::Result<Vec<arrow::record_batch::RecordBatch>, ApiaryError>> {
        let timeout = self.default_timeout;
        let queue = Arc::clone(&self.queue);
        let bees = self.bees.clone();
        let default_timeout = self.default_timeout;

        tokio::task::spawn(async move {
            // Execute with timeout
            let result = tokio::time::timeout(timeout, tokio::task::spawn_blocking(func)).await;

            // Reset chamber (clear memory tracking + scratch dir)
            bee.chamber.reset();

            // Mark bee idle
            {
                let mut state = bee.state.lock().await;
                *state = BeeState::Idle;
            }

            // Try to drain the queue onto idle bees
            drain_queue_once(queue, bees, default_timeout).await;

            match result {
                Ok(Ok(task_result)) => task_result,
                Ok(Err(join_err)) => Err(ApiaryError::Internal {
                    message: format!("Task panicked: {join_err}"),
                }),
                Err(_) => Err(ApiaryError::TaskTimeout {
                    message: format!("Task {task_id} exceeded {timeout:?} timeout"),
                }),
            }
        })
    }
}

/// Try to assign one queued task to an idle bee.
/// Uses explicit boxing for the recursive async call.
fn drain_queue_once(
    queue: Arc<Mutex<VecDeque<QueuedTask>>>,
    bees: Vec<Arc<Bee>>,
    timeout: Duration,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        // Find an idle bee
        let mut idle_bee = None;
        for bee in &bees {
            let state = bee.state.lock().await;
            if *state == BeeState::Idle {
                idle_bee = Some(Arc::clone(bee));
                break;
            }
        }

        let Some(bee) = idle_bee else {
            return;
        };

        // Pop a queued task
        let queued = {
            let mut q = queue.lock().await;
            q.pop_front()
        };

        let Some(queued) = queued else {
            return;
        };

        // Mark bee busy
        {
            let mut state = bee.state.lock().await;
            *state = BeeState::Busy(queued.task_id.clone());
        }

        let task_id = queued.task_id;
        let func = queued.func;
        let tx = queued.tx;
        let queue_clone = Arc::clone(&queue);
        let bees_clone = bees.clone();

        // Spawn the queued task
        tokio::task::spawn(async move {
            let result =
                tokio::time::timeout(timeout, tokio::task::spawn_blocking(func)).await;

            bee.chamber.reset();
            {
                let mut state = bee.state.lock().await;
                *state = BeeState::Idle;
            }

            let final_result = match result {
                Ok(Ok(task_result)) => task_result,
                Ok(Err(join_err)) => Err(ApiaryError::Internal {
                    message: format!("Task panicked: {join_err}"),
                }),
                Err(_) => Err(ApiaryError::TaskTimeout {
                    message: format!("Task {task_id} exceeded {timeout:?} timeout"),
                }),
            };

            let _ = tx.send(final_result);

            // Continue draining
            drain_queue_once(queue_clone, bees_clone, timeout).await;
        });
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn test_config(cores: usize) -> (NodeConfig, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::detect("local://test");
        config.cores = cores;
        config.memory_per_bee = 1024 * 1024; // 1 MB per bee for testing
        config.cache_dir = tmp.path().to_path_buf();
        (config, tmp)
    }

    #[tokio::test]
    async fn test_bee_pool_creates_correct_number_of_bees() {
        let (config, _tmp) = test_config(4);
        let pool = BeePool::new(&config);
        assert_eq!(pool.bee_count(), 4);
        let status = pool.status().await;
        assert_eq!(status.len(), 4);
        for s in &status {
            assert_eq!(s.state, "idle");
            assert_eq!(s.memory_budget, 1024 * 1024);
            assert_eq!(s.memory_used, 0);
        }
    }

    #[tokio::test]
    async fn test_bee_pool_executes_task() {
        let (config, _tmp) = test_config(2);
        let pool = BeePool::new(&config);

        let handle = pool.submit(|| Ok(vec![])).await;
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_bee_returns_to_idle_after_task() {
        let (config, _tmp) = test_config(1);
        let pool = BeePool::new(&config);

        let handle = pool.submit(|| Ok(vec![])).await;
        handle.await.unwrap().unwrap();

        // Allow the pool to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = pool.status().await;
        assert_eq!(status[0].state, "idle");
    }

    #[tokio::test]
    async fn test_memory_enforcement() {
        let bee_id = BeeId::new("test-bee");
        let tmp = tempfile::TempDir::new().unwrap();
        let chamber = MasonChamber::new(
            bee_id.clone(),
            1000, // 1000 bytes budget
            tmp.path().to_path_buf(),
            Duration::from_secs(10),
        );

        // Should succeed
        assert!(chamber.request_memory(500).is_ok());
        assert_eq!(chamber.memory_used.load(Ordering::Relaxed), 500);

        // Should succeed (total 900)
        assert!(chamber.request_memory(400).is_ok());
        assert_eq!(chamber.memory_used.load(Ordering::Relaxed), 900);

        // Should fail (would exceed 1000)
        let err = chamber.request_memory(200);
        assert!(err.is_err());
        match err.unwrap_err() {
            ApiaryError::MemoryExceeded { bee_id: id, budget, requested } => {
                assert_eq!(id, bee_id);
                assert_eq!(budget, 1000);
                assert_eq!(requested, 200);
            }
            other => panic!("Expected MemoryExceeded, got: {:?}", other),
        }

        // Memory should not have changed
        assert_eq!(chamber.memory_used.load(Ordering::Relaxed), 900);

        // Release and try again
        chamber.release_memory(400);
        assert_eq!(chamber.memory_used.load(Ordering::Relaxed), 500);
        assert!(chamber.request_memory(200).is_ok());
    }

    #[tokio::test]
    async fn test_memory_exceeded_does_not_affect_other_bees() {
        let (config, _tmp) = test_config(2);
        let pool = BeePool::new(&config);

        let budget = config.memory_per_bee;

        // Submit task that fails with memory exceeded on one bee
        let handle = pool.submit(move || {
            Err(ApiaryError::MemoryExceeded {
                bee_id: BeeId::new("bee-0"),
                budget,
                requested: budget + 1,
            })
        }).await;

        let result = handle.await.unwrap();
        assert!(result.is_err());

        // Allow drain
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Other bee should still work fine
        let handle2 = pool.submit(|| Ok(vec![])).await;
        let result2 = handle2.await.unwrap();
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_task_timeout() {
        let (config, _tmp) = test_config(1);
        let mut pool = BeePool::new(&config);
        pool.default_timeout = Duration::from_millis(100);

        let handle = pool.submit(|| {
            std::thread::sleep(Duration::from_secs(5));
            Ok(vec![])
        }).await;

        let result = handle.await.unwrap();
        assert!(result.is_err());
        match result.unwrap_err() {
            ApiaryError::TaskTimeout { message } => {
                assert!(message.contains("timeout"), "Got: {message}");
            }
            other => panic!("Expected TaskTimeout, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_scratch_directory_isolated_and_cleaned() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = NodeConfig::detect("local://test");
        config.cores = 2;
        config.memory_per_bee = 1024 * 1024;
        config.cache_dir = tmp.path().to_path_buf();

        let pool = BeePool::new(&config);

        // Each bee should have its own scratch dir
        let scratch_0 = tmp.path().join("scratch").join("bee_0");
        let scratch_1 = tmp.path().join("scratch").join("bee_1");
        assert!(scratch_0.exists());
        assert!(scratch_1.exists());

        // Write a file in bee-0's scratch
        let scratch_0_clone = scratch_0.clone();
        let handle = pool.submit(move || {
            std::fs::write(scratch_0_clone.join("test.tmp"), b"hello").unwrap();
            Ok(vec![])
        }).await;
        handle.await.unwrap().unwrap();

        // After task completes, scratch should be cleaned
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The directory should exist (recreated) but be empty
        assert!(scratch_0.exists());
        let entries: Vec<_> = std::fs::read_dir(&scratch_0).unwrap().collect();
        assert!(entries.is_empty(), "Scratch dir should be cleaned after task");
    }

    #[tokio::test]
    async fn test_concurrent_tasks_on_separate_bees() {
        let (config, _tmp) = test_config(3);
        let pool = Arc::new(BeePool::new(&config));

        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..3 {
            let c = counter.clone();
            let h = pool.submit(move || {
                c.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(50));
                Ok(vec![])
            }).await;
            handles.push(h);
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_tasks_queue_when_all_bees_busy() {
        let (config, _tmp) = test_config(1); // Only 1 bee
        let pool = Arc::new(BeePool::new(&config));

        let counter = Arc::new(AtomicUsize::new(0));

        // Submit 3 tasks to 1 bee — 2 should queue
        let mut handles = vec![];
        for _ in 0..3 {
            let c = counter.clone();
            let h = pool.submit(move || {
                c.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(20));
                Ok(vec![])
            }).await;
            handles.push(h);
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }

        // All 3 tasks should have completed
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_chamber_utilisation() {
        let bee_id = BeeId::new("test-bee");
        let tmp = tempfile::TempDir::new().unwrap();
        let chamber = MasonChamber::new(
            bee_id,
            1000,
            tmp.path().to_path_buf(),
            Duration::from_secs(10),
        );

        assert!((chamber.utilisation() - 0.0).abs() < f64::EPSILON);
        chamber.request_memory(500).unwrap();
        assert!((chamber.utilisation() - 0.5).abs() < f64::EPSILON);
        chamber.request_memory(500).unwrap();
        assert!((chamber.utilisation() - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_chamber_reset() {
        let bee_id = BeeId::new("test-bee");
        let tmp = tempfile::TempDir::new().unwrap();
        let scratch = tmp.path().join("scratch");
        std::fs::create_dir_all(&scratch).unwrap();
        std::fs::write(scratch.join("leftover.tmp"), b"data").unwrap();

        let chamber = MasonChamber::new(
            bee_id,
            1000,
            scratch.clone(),
            Duration::from_secs(10),
        );
        chamber.request_memory(800).unwrap();
        assert_eq!(chamber.memory_used.load(Ordering::Relaxed), 800);

        chamber.reset();
        assert_eq!(chamber.memory_used.load(Ordering::Relaxed), 0);
        assert!(scratch.exists());
        let entries: Vec<_> = std::fs::read_dir(&scratch).unwrap().collect();
        assert!(entries.is_empty());
    }
}
