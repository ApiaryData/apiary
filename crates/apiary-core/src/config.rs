//! Node configuration with automatic system detection.
//!
//! [`NodeConfig`] captures a node's identity, storage configuration,
//! hardware capacity, and tuning parameters. The [`NodeConfig::detect`]
//! constructor auto-detects cores and memory from the system.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Configuration for an Apiary node, including auto-detected system capacity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique identifier for this node.
    pub node_id: NodeId,

    /// Storage URI: `"local://~/.apiary/data"` or `"s3://bucket/prefix"`.
    pub storage_uri: String,

    /// Local directory for cell cache and scratch files.
    pub cache_dir: PathBuf,

    /// Number of CPU cores (auto-detected).
    pub cores: usize,

    /// Total memory in bytes (auto-detected).
    pub memory_bytes: u64,

    /// Memory budget per bee in bytes (`memory_bytes / cores`).
    pub memory_per_bee: u64,

    /// Target cell size in bytes for leafcutter sizing (`memory_per_bee / 4`).
    pub target_cell_size: u64,

    /// Maximum cell size in bytes (`target_cell_size * 2`).
    pub max_cell_size: u64,

    /// Minimum cell size in bytes (floor to amortise S3 overhead).
    pub min_cell_size: u64,

    /// Interval between heartbeat writes.
    pub heartbeat_interval: Duration,

    /// Duration after which a node with no heartbeat is considered dead.
    pub dead_threshold: Duration,
}

/// Minimum cell size floor: 16 MB.
const MIN_CELL_SIZE_FLOOR: u64 = 16 * 1024 * 1024;

/// Default heartbeat interval: 5 seconds.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Default dead threshold: 30 seconds (6 missed heartbeats).
const DEFAULT_DEAD_THRESHOLD: Duration = Duration::from_secs(30);

impl NodeConfig {
    /// Create a new `NodeConfig` by auto-detecting system resources.
    ///
    /// # Arguments
    ///
    /// * `storage_uri` — The storage backend URI (e.g., `"local://~/.apiary/data"`
    ///   or `"s3://bucket/prefix"`).
    ///
    /// # Example
    ///
    /// ```
    /// use apiary_core::config::NodeConfig;
    ///
    /// let config = NodeConfig::detect("local://~/.apiary/data");
    /// assert!(config.cores > 0);
    /// assert!(config.memory_bytes > 0);
    /// ```
    pub fn detect(storage_uri: impl Into<String>) -> Self {
        let storage_uri = storage_uri.into();
        let node_id = NodeId::generate();

        let cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        // Detect available system memory.
        let memory_bytes = detect_memory();

        let memory_per_bee = memory_bytes / cores as u64;
        let target_cell_size = memory_per_bee / 4;
        let max_cell_size = target_cell_size * 2;
        let min_cell_size = MIN_CELL_SIZE_FLOOR;

        let cache_dir = dirs_cache_dir().join("apiary").join("cache");

        Self {
            node_id,
            storage_uri,
            cache_dir,
            cores,
            memory_bytes,
            memory_per_bee,
            target_cell_size,
            max_cell_size,
            min_cell_size,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            dead_threshold: DEFAULT_DEAD_THRESHOLD,
        }
    }
}

/// Detect total system memory in bytes.
/// Falls back to 1 GB if detection fails.
fn detect_memory() -> u64 {
    // Use platform-specific detection
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    let rest = rest.trim();
                    if let Some(kb_str) = rest.strip_suffix("kB") {
                        if let Ok(kb) = kb_str.trim().parse::<u64>() {
                            return kb * 1024;
                        }
                    }
                }
            }
        }
    }

    #[cfg(target_os = "windows")]
    {
        use std::mem;

        #[repr(C)]
        #[allow(non_snake_case)]
        struct MEMORYSTATUSEX {
            dwLength: u32,
            dwMemoryLoad: u32,
            ullTotalPhys: u64,
            ullAvailPhys: u64,
            ullTotalPageFile: u64,
            ullAvailPageFile: u64,
            ullTotalVirtual: u64,
            ullAvailVirtual: u64,
            ullAvailExtendedVirtual: u64,
        }

        extern "system" {
            fn GlobalMemoryStatusEx(lpBuffer: *mut MEMORYSTATUSEX) -> i32;
        }

        unsafe {
            let mut status: MEMORYSTATUSEX = mem::zeroed();
            status.dwLength = mem::size_of::<MEMORYSTATUSEX>() as u32;
            if GlobalMemoryStatusEx(&mut status) != 0 {
                return status.ullTotalPhys;
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        use std::mem;

        extern "C" {
            fn sysctl(
                name: *const i32,
                namelen: u32,
                oldp: *mut std::ffi::c_void,
                oldlenp: *mut usize,
                newp: *const std::ffi::c_void,
                newlen: usize,
            ) -> i32;
        }

        unsafe {
            // CTL_HW = 6, HW_MEMSIZE = 24
            let mib: [i32; 2] = [6, 24];
            let mut memsize: u64 = 0;
            let mut len = mem::size_of::<u64>();
            if sysctl(
                mib.as_ptr(),
                2,
                &mut memsize as *mut u64 as *mut std::ffi::c_void,
                &mut len,
                std::ptr::null(),
                0,
            ) == 0
            {
                return memsize;
            }
        }
    }

    // Fallback: 1 GB
    1024 * 1024 * 1024
}

/// Return a suitable base directory for cache, falling back to `.` if the
/// home directory cannot be determined.
fn dirs_cache_dir() -> PathBuf {
    home_dir().unwrap_or_else(|| PathBuf::from("."))
}

/// Best-effort home directory detection without pulling in a heavy crate.
fn home_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        std::env::var("USERPROFILE").ok().map(PathBuf::from)
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var("HOME").ok().map(PathBuf::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_creates_valid_config() {
        let config = NodeConfig::detect("local://test");
        assert!(config.cores > 0, "cores must be > 0");
        assert!(config.memory_bytes > 0, "memory must be > 0");
        assert_eq!(
            config.memory_per_bee,
            config.memory_bytes / config.cores as u64
        );
        assert_eq!(config.target_cell_size, config.memory_per_bee / 4);
        assert_eq!(config.max_cell_size, config.target_cell_size * 2);
        assert_eq!(config.min_cell_size, MIN_CELL_SIZE_FLOOR);
        assert_eq!(config.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL);
        assert_eq!(config.dead_threshold, DEFAULT_DEAD_THRESHOLD);
    }

    #[test]
    fn test_detect_unique_node_ids() {
        let c1 = NodeConfig::detect("local://a");
        let c2 = NodeConfig::detect("local://b");
        assert_ne!(c1.node_id, c2.node_id);
    }

    #[test]
    fn test_config_serialization() {
        let config = NodeConfig::detect("s3://bucket/prefix");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: NodeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.storage_uri, config.storage_uri);
        assert_eq!(deserialized.cores, config.cores);
    }
}
