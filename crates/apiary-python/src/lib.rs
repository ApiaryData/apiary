//! Python bindings for the Apiary runtime via PyO3.
//!
//! Provides the `Apiary` class which is the main entry point for the
//! Python SDK. Wraps [`ApiaryNode`](apiary_runtime::ApiaryNode) with
//! a Tokio runtime for async operations.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Mutex;

use apiary_core::config::NodeConfig;
use apiary_runtime::ApiaryNode;

/// The main Apiary Python SDK class.
///
/// # Example (Python)
///
/// ```python
/// from apiary import Apiary
///
/// ap = Apiary("production")
/// ap.start()
/// print(ap.status())
/// ap.shutdown()
/// ```
#[pyclass]
struct Apiary {
    name: String,
    storage_uri: String,
    node: Mutex<Option<ApiaryNode>>,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl Apiary {
    /// Create a new Apiary instance.
    ///
    /// Args:
    ///     name: The apiary name (used for logging and identification).
    ///     storage: Optional storage URI. Defaults to local filesystem at `~/.apiary/data/<name>`.
    #[new]
    #[pyo3(signature = (name, storage=None))]
    fn new(name: String, storage: Option<String>) -> PyResult<Self> {
        let storage_uri =
            storage.unwrap_or_else(|| format!("local://~/.apiary/data/{name}"));

        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {e}"))
        })?;

        Ok(Self {
            name,
            storage_uri,
            node: Mutex::new(None),
            runtime,
        })
    }

    /// Start the Apiary node.
    ///
    /// Initialises the storage backend, detects system capacity, and begins
    /// operation. Must be called before any other operations.
    fn start(&self) -> PyResult<()> {
        // Initialise tracing subscriber (idempotent)
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .try_init();

        let config = NodeConfig::detect(&self.storage_uri);
        let node = self
            .runtime
            .block_on(async { ApiaryNode::start(config).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to start node: {e}")))?;

        let mut guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        *guard = Some(node);

        Ok(())
    }

    /// Return the current node status as a dictionary.
    ///
    /// Returns:
    ///     dict: Node status including node_id, cores, memory_gb, bees, and storage type.
    fn status(&self) -> PyResult<PyObject> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        let storage_type = if node.config.storage_uri.starts_with("s3://") {
            "s3"
        } else {
            "local"
        };

        Python::with_gil(|py| {
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("name", &self.name)?;
            dict.set_item("node_id", node.config.node_id.as_str())?;
            dict.set_item("cores", node.config.cores)?;
            dict.set_item(
                "memory_gb",
                node.config.memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            )?;
            dict.set_item("bees", node.config.cores)?;
            dict.set_item("storage", storage_type)?;
            dict.set_item(
                "memory_per_bee_mb",
                node.config.memory_per_bee / (1024 * 1024),
            )?;
            dict.set_item(
                "target_cell_size_mb",
                node.config.target_cell_size / (1024 * 1024),
            )?;
            Ok(dict.into())
        })
    }

    /// Gracefully shut down the Apiary node.
    fn shutdown(&self) -> PyResult<()> {
        let mut guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        if let Some(node) = guard.take() {
            self.runtime.block_on(async {
                node.shutdown().await;
            });
        }
        Ok(())
    }
}

/// The `apiary` Python module.
#[pymodule]
fn apiary(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Apiary>()?;
    Ok(())
}
