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
            let dict = pyo3::types::PyDict::new_bound(py);
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

    /// Create a hive (database).
    ///
    /// Args:
    ///     name: The hive name.
    ///
    /// Returns:
    ///     None. Raises an exception if the operation fails.
    fn create_hive(&self, name: String) -> PyResult<()> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        self.runtime
            .block_on(async { node.registry.create_hive(&name).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create hive: {e}")))?;

        Ok(())
    }

    /// Create a box (schema) within a hive.
    ///
    /// Args:
    ///     hive: The hive name.
    ///     name: The box name.
    ///
    /// Returns:
    ///     None. Raises an exception if the operation fails.
    fn create_box(&self, hive: String, name: String) -> PyResult<()> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        self.runtime
            .block_on(async { node.registry.create_box(&hive, &name).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create box: {e}")))?;

        Ok(())
    }

    /// Create a frame (table) within a box.
    ///
    /// Args:
    ///     hive: The hive name.
    ///     box_name: The box name.
    ///     name: The frame name.
    ///     schema: The schema as a dict (Arrow schema JSON format).
    ///     partition_by: Optional list of column names to partition by.
    ///
    /// Returns:
    ///     None. Raises an exception if the operation fails.
    #[pyo3(signature = (hive, box_name, name, schema, partition_by=None))]
    fn create_frame(
        &self,
        hive: String,
        box_name: String,
        name: String,
        schema: Bound<'_, PyAny>,
        partition_by: Option<Vec<String>>,
    ) -> PyResult<()> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        // Convert Python dict to JSON value
        let schema_json: serde_json::Value = pythonize::depythonize(&schema)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid schema: {e}")))?;

        let partition_by = partition_by.unwrap_or_default();

        self.runtime
            .block_on(async {
                node.registry
                    .create_frame(&hive, &box_name, &name, schema_json, partition_by)
                    .await
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create frame: {e}")))?;

        Ok(())
    }

    /// List all hives.
    ///
    /// Returns:
    ///     list[str]: A list of hive names.
    fn list_hives(&self) -> PyResult<Vec<String>> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        self.runtime
            .block_on(async { node.registry.list_hives().await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to list hives: {e}")))
    }

    /// List all boxes in a hive.
    ///
    /// Args:
    ///     hive: The hive name.
    ///
    /// Returns:
    ///     list[str]: A list of box names.
    fn list_boxes(&self, hive: String) -> PyResult<Vec<String>> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        self.runtime
            .block_on(async { node.registry.list_boxes(&hive).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to list boxes: {e}")))
    }

    /// List all frames in a box.
    ///
    /// Args:
    ///     hive: The hive name.
    ///     box_name: The box name.
    ///
    /// Returns:
    ///     list[str]: A list of frame names.
    fn list_frames(&self, hive: String, box_name: String) -> PyResult<Vec<String>> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        self.runtime
            .block_on(async { node.registry.list_frames(&hive, &box_name).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to list frames: {e}")))
    }

    /// Get frame metadata.
    ///
    /// Args:
    ///     hive: The hive name.
    ///     box_name: The box name.
    ///     name: The frame name.
    ///
    /// Returns:
    ///     dict: Frame metadata including schema and partition columns.
    fn get_frame(&self, hive: String, box_name: String, name: String) -> PyResult<PyObject> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        let frame = self
            .runtime
            .block_on(async { node.registry.get_frame(&hive, &box_name, &name).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get frame: {e}")))?;

        Python::with_gil(|py| {
            let dict = pyo3::types::PyDict::new_bound(py);
            dict.set_item("schema", pythonize::pythonize(py, &frame.schema)?)?;
            dict.set_item("partition_by", frame.partition_by)?;
            dict.set_item("max_partitions", frame.max_partitions)?;
            dict.set_item("created_at", frame.created_at.to_rfc3339())?;
            Ok(dict.into())
        })
    }
}

/// The `apiary` Python module.
#[pymodule]
fn apiary(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Apiary>()?;
    Ok(())
}
