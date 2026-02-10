//! Python bindings for the Apiary runtime via PyO3.
//!
//! Provides the `Apiary` class which is the main entry point for the
//! Python SDK. Wraps [`ApiaryNode`](apiary_runtime::ApiaryNode) with
//! a Tokio runtime for async operations.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::collections::HashMap;
use std::sync::Mutex;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

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

        // Initialize the frame's ledger so DESCRIBE works immediately
        self.runtime
            .block_on(async {
                node.init_frame_ledger(&hive, &box_name, &name).await
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to init frame ledger: {e}")))?;

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

    /// Write data to a frame.
    ///
    /// Data is passed as Arrow IPC stream bytes (serialized PyArrow table).
    /// In Python, use: `frame.write(table)` which handles serialization.
    ///
    /// Args:
    ///     hive: The hive name.
    ///     box_name: The box name.
    ///     frame_name: The frame name.
    ///     ipc_data: Arrow IPC stream bytes from PyArrow.
    ///
    /// Returns:
    ///     dict: WriteResult with version, cells_written, rows_written, bytes_written, duration_ms.
    fn write_to_frame(
        &self,
        hive: String,
        box_name: String,
        frame_name: String,
        ipc_data: &Bound<'_, PyBytes>,
    ) -> PyResult<PyObject> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        // Deserialize Arrow IPC bytes to RecordBatch
        let data = ipc_data.as_bytes();
        let batch = ipc_bytes_to_batch(data)?;

        let result = self
            .runtime
            .block_on(async {
                node.write_to_frame(&hive, &box_name, &frame_name, &batch)
                    .await
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to write: {e}")))?;

        Python::with_gil(|py| {
            let dict = pyo3::types::PyDict::new_bound(py);
            dict.set_item("version", result.version)?;
            dict.set_item("cells_written", result.cells_written)?;
            dict.set_item("rows_written", result.rows_written)?;
            dict.set_item("bytes_written", result.bytes_written)?;
            dict.set_item("duration_ms", result.duration_ms)?;
            dict.set_item("temperature", result.temperature)?;
            Ok(dict.into())
        })
    }

    /// Read data from a frame.
    ///
    /// Returns Arrow IPC stream bytes that can be deserialized by PyArrow.
    ///
    /// Args:
    ///     hive: The hive name.
    ///     box_name: The box name.
    ///     frame_name: The frame name.
    ///     partition_filter: Optional dict of partition column → value for pruning.
    ///
    /// Returns:
    ///     bytes: Arrow IPC stream bytes, or None if no data.
    #[pyo3(signature = (hive, box_name, frame_name, partition_filter=None))]
    fn read_from_frame(
        &self,
        hive: String,
        box_name: String,
        frame_name: String,
        partition_filter: Option<HashMap<String, String>>,
    ) -> PyResult<PyObject> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        let result = self
            .runtime
            .block_on(async {
                node.read_from_frame(
                    &hive,
                    &box_name,
                    &frame_name,
                    partition_filter.as_ref(),
                )
                .await
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to read: {e}")))?;

        Python::with_gil(|py| match result {
            Some(batch) => {
                let ipc_data = batch_to_ipc_bytes(&batch)?;
                let py_bytes = PyBytes::new_bound(py, &ipc_data);
                Ok(py_bytes.into())
            }
            None => Ok(py.None()),
        })
    }

    /// Overwrite all data in a frame.
    ///
    /// Args:
    ///     hive: The hive name.
    ///     box_name: The box name.
    ///     frame_name: The frame name.
    ///     ipc_data: Arrow IPC stream bytes from PyArrow.
    ///
    /// Returns:
    ///     dict: WriteResult with version, cells_written, etc.
    fn overwrite_frame(
        &self,
        hive: String,
        box_name: String,
        frame_name: String,
        ipc_data: &Bound<'_, PyBytes>,
    ) -> PyResult<PyObject> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        let data = ipc_data.as_bytes();
        let batch = ipc_bytes_to_batch(data)?;

        let result = self
            .runtime
            .block_on(async {
                node.overwrite_frame(&hive, &box_name, &frame_name, &batch)
                    .await
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to overwrite: {e}")))?;

        Python::with_gil(|py| {
            let dict = pyo3::types::PyDict::new_bound(py);
            dict.set_item("version", result.version)?;
            dict.set_item("cells_written", result.cells_written)?;
            dict.set_item("rows_written", result.rows_written)?;
            dict.set_item("bytes_written", result.bytes_written)?;
            dict.set_item("duration_ms", result.duration_ms)?;
            dict.set_item("temperature", result.temperature)?;
            Ok(dict.into())
        })
    }

    /// Execute a SQL query and return results as Arrow IPC bytes.
    ///
    /// Supports standard SQL (SELECT, GROUP BY, etc.) and custom commands
    /// (USE HIVE, USE BOX, SHOW HIVES, SHOW BOXES, SHOW FRAMES, DESCRIBE).
    ///
    /// Table references can be fully qualified (hive.box.frame) or use the
    /// current context set by USE HIVE / USE BOX.
    ///
    /// Args:
    ///     query: SQL query string.
    ///
    /// Returns:
    ///     bytes: Arrow IPC stream bytes (deserialize with PyArrow), or None if empty result.
    fn sql(&self, query: String) -> PyResult<PyObject> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        let batches = self
            .runtime
            .block_on(async { node.sql(&query).await })
            .map_err(|e| PyRuntimeError::new_err(format!("SQL error: {e}")))?;

        Python::with_gil(|py| {
            if batches.is_empty() {
                return Ok(py.None());
            }

            // Concatenate all batches into one
            let schema = batches[0].schema();
            let merged = if batches.len() == 1 {
                batches.into_iter().next().unwrap()
            } else {
                arrow::compute::concat_batches(&schema, &batches).map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to merge results: {e}"))
                })?
            };

            let ipc_data = batch_to_ipc_bytes(&merged)?;
            let py_bytes = PyBytes::new_bound(py, &ipc_data);
            Ok(py_bytes.into())
        })
    }

    // --- Dual terminology aliases (database/schema/table) ---

    /// Return the status of each bee in the pool.
    ///
    /// Returns:
    ///     list[dict]: A list of bee status dicts, each with bee_id, state, memory_used, memory_budget.
    fn bee_status(&self) -> PyResult<PyObject> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        let statuses = self
            .runtime
            .block_on(async { node.bee_status().await });

        Python::with_gil(|py| {
            let list = pyo3::types::PyList::empty_bound(py);
            for s in statuses {
                let dict = pyo3::types::PyDict::new_bound(py);
                dict.set_item("bee_id", &s.bee_id)?;
                dict.set_item("state", &s.state)?;
                dict.set_item("memory_used", s.memory_used)?;
                dict.set_item("memory_budget", s.memory_budget)?;
                list.append(dict)?;
            }
            Ok(list.into())
        })
    }

    /// Return the swarm status: all nodes visible to this node.
    ///
    /// Returns:
    ///     dict: Swarm status with 'nodes' (list of node info dicts),
    ///           'total_bees', and 'total_idle_bees'.
    fn swarm_status(&self) -> PyResult<PyObject> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        let status = self
            .runtime
            .block_on(async { node.swarm_status().await });

        Python::with_gil(|py| {
            let nodes_list = pyo3::types::PyList::empty_bound(py);
            for n in &status.nodes {
                let d = pyo3::types::PyDict::new_bound(py);
                d.set_item("node_id", &n.node_id)?;
                d.set_item("state", &n.state)?;
                d.set_item("bees", n.bees)?;
                d.set_item("idle_bees", n.idle_bees)?;
                d.set_item("memory_pressure", n.memory_pressure)?;
                d.set_item("colony_temperature", n.colony_temperature)?;
                nodes_list.append(d)?;
            }
            let result = pyo3::types::PyDict::new_bound(py);
            result.set_item("nodes", nodes_list)?;
            result.set_item("total_bees", status.total_bees)?;
            result.set_item("total_idle_bees", status.total_idle_bees)?;
            Ok(result.into())
        })
    }

    /// Return the colony status: temperature and regulation state.
    ///
    /// Returns:
    ///     dict: Colony status with 'temperature' (0.0-1.0), 'regulation'
    ///           ("cold"/"ideal"/"warm"/"hot"/"critical"), and 'setpoint'.
    fn colony_status(&self) -> PyResult<PyObject> {
        let guard = self
            .node
            .lock()
            .map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {e}")))?;
        let node = guard.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Node not started. Call start() first.")
        })?;

        let status = self
            .runtime
            .block_on(async { node.colony_status().await });

        Python::with_gil(|py| {
            let dict = pyo3::types::PyDict::new_bound(py);
            dict.set_item("temperature", status.temperature)?;
            dict.set_item("regulation", &status.regulation)?;
            dict.set_item("setpoint", status.setpoint)?;
            Ok(dict.into())
        })
    }

    /// Alias for create_hive (traditional database terminology).
    fn create_database(&self, name: String) -> PyResult<()> {
        self.create_hive(name)
    }

    /// Alias for create_box (traditional database terminology).
    fn create_schema(&self, database: String, name: String) -> PyResult<()> {
        self.create_box(database, name)
    }

    /// Alias for create_frame (traditional database terminology).
    #[pyo3(signature = (database, schema, name, columns, partition_by=None))]
    fn create_table(
        &self,
        database: String,
        schema: String,
        name: String,
        columns: Bound<'_, PyAny>,
        partition_by: Option<Vec<String>>,
    ) -> PyResult<()> {
        self.create_frame(database, schema, name, columns, partition_by)
    }

    /// Alias for list_hives.
    fn list_databases(&self) -> PyResult<Vec<String>> {
        self.list_hives()
    }

    /// Alias for list_boxes.
    fn list_schemas(&self, database: String) -> PyResult<Vec<String>> {
        self.list_boxes(database)
    }

    /// Alias for list_frames.
    fn list_tables(&self, database: String, schema: String) -> PyResult<Vec<String>> {
        self.list_frames(database, schema)
    }

    /// Alias for get_frame.
    fn get_table(&self, database: String, schema: String, name: String) -> PyResult<PyObject> {
        self.get_frame(database, schema, name)
    }
}

/// Deserialize Arrow IPC stream bytes into a single RecordBatch.
/// If the stream contains multiple batches, they are merged into one.
fn ipc_bytes_to_batch(data: &[u8]) -> PyResult<RecordBatch> {
    let cursor = std::io::Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to read Arrow IPC: {e}")))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to read batch: {e}")))?;
        batches.push(batch);
    }

    if batches.is_empty() {
        return Err(PyRuntimeError::new_err("No data in IPC stream"));
    }

    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    // Merge multiple batches
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to merge batches: {e}")))
}

/// Serialize a RecordBatch to Arrow IPC stream bytes for transfer to Python.
fn batch_to_ipc_bytes(batch: &RecordBatch) -> PyResult<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create IPC writer: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to write IPC: {e}")))?;
        writer
            .finish()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to finish IPC: {e}")))?;
    }
    Ok(buf)
}

/// The `apiary` Python module.
#[pymodule]
fn apiary(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Apiary>()?;
    Ok(())
}
