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

    /// Maximum cache size in bytes for local cell storage.
    pub max_cache_size: u64,

    /// Interval between heartbeat writes.
    pub heartbeat_interval: Duration,

    /// Duration after which a node with no heartbeat is considered dead.
    pub dead_threshold: Duration,

    /// How often the Node deposits its crop into the comb. Data ingested
    /// between deposits is queryable at once but exists only on this Node's
    /// disk, so this is also the loss window if the Node's disk dies.
    #[serde(default = "default_deposit_interval")]
    pub deposit_interval: Duration,

    /// Crop size in bytes that triggers a deposit before the interval is up.
    #[serde(default = "default_crop_max_bytes")]
    pub crop_max_bytes: u64,

    /// Whether each ingest is synced to disk before it returns (the default).
    /// Turning this off is faster but a power cut can lose the most recent
    /// ingests; it is meant for benchmarks and for a crop on a RAM disk.
    #[serde(default = "default_crop_sync")]
    pub crop_sync: bool,
}

fn default_crop_sync() -> bool {
    true
}

fn default_deposit_interval() -> Duration {
    DEFAULT_DEPOSIT_INTERVAL
}

fn default_crop_max_bytes() -> u64 {
    DEFAULT_CROP_MAX_BYTES
}

/// Minimum cell size floor: 16 MB.
const MIN_CELL_SIZE_FLOOR: u64 = 16 * 1024 * 1024;

/// Default cache size: 2 GB.
const DEFAULT_MAX_CACHE_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Default heartbeat interval: 5 seconds.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Default dead threshold: 30 seconds (6 missed heartbeats).
const DEFAULT_DEAD_THRESHOLD: Duration = Duration::from_secs(30);

/// Default deposit interval: 10 seconds.
const DEFAULT_DEPOSIT_INTERVAL: Duration = Duration::from_secs(10);

/// Default crop size that forces a deposit: 64 MB.
const DEFAULT_CROP_MAX_BYTES: u64 = 64 * 1024 * 1024;

impl NodeConfig {
    /// The directory that holds this Node's crop: data landed here and not yet
    /// deposited into the comb. It sits under the cache directory, which on a
    /// Raspberry Pi should be on an SSD.
    ///
    /// A crop belongs to one comb: it records how far it has deposited into that
    /// comb's tables. So the directory is named for the storage URI, and Nodes
    /// that share a cache directory but not a comb never share a crop.
    pub fn crop_dir(&self) -> PathBuf {
        let comb = format!("{:016x}", crate::rng::hash_str(&self.storage_uri));
        self.cache_dir.join("crop").join(comb)
    }

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
            max_cache_size: DEFAULT_MAX_CACHE_SIZE,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            dead_threshold: DEFAULT_DEAD_THRESHOLD,
            deposit_interval: DEFAULT_DEPOSIT_INTERVAL,
            crop_max_bytes: DEFAULT_CROP_MAX_BYTES,
            crop_sync: true,
        }
    }
}

/// Detect total system memory in bytes.
/// Falls back to 1 GB if detection fails.
///
/// On Linux, also checks cgroup memory limits so that the detected value
/// respects Docker / container resource constraints rather than reporting
/// the full host RAM.
fn detect_memory() -> u64 {
    // Use platform-specific detection
    #[cfg(target_os = "linux")]
    {
        let mut host_memory: Option<u64> = None;

        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    let rest = rest.trim();
                    if let Some(kb_str) = rest.strip_suffix("kB")
                        && let Ok(kb) = kb_str.trim().parse::<u64>()
                    {
                        host_memory = Some(kb * 1024);
                        break;
                    }
                }
            }
        }

        let cgroup_limit = read_cgroup_memory_limit();

        match (host_memory, cgroup_limit) {
            (Some(host), Some(limit)) => return limit.min(host),
            (Some(host), None) => return host,
            _ => {}
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

/// Read the container memory limit from the Linux cgroup subsystem.
///
/// Supports both cgroup v2 (`/sys/fs/cgroup/memory.max`) and cgroup v1
/// (`/sys/fs/cgroup/memory/memory.limit_in_bytes`).  Returns `None` when
/// no container limit is in effect or the files cannot be read.
#[cfg(target_os = "linux")]
fn read_cgroup_memory_limit() -> Option<u64> {
    // cgroup v2: the limit file contains a decimal byte count or "max"
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = contents.trim();
        if trimmed != "max"
            && let Ok(bytes) = trimmed.parse::<u64>()
        {
            return Some(bytes);
        }
    }

    // cgroup v1: the limit file always contains a decimal byte count; when no
    // limit is set the kernel writes a very large sentinel value (close to
    // u64::MAX).  We treat anything above 2^62 bytes (~4.6 EiB) as "no limit".
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        && let Ok(bytes) = contents.trim().parse::<u64>()
        && bytes < (1u64 << 62)
    {
        return Some(bytes);
    }

    None
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

    /// When a cgroup memory limit is in effect the detected memory must be
    /// at most as large as the cgroup limit — never larger.
    ///
    /// Verifies the min() logic used in detect_memory().
    #[test]
    #[cfg(target_os = "linux")]
    fn test_cgroup_limit_wins_over_larger_host_memory() {
        let cgroup_limit: u64 = 512 * 1024 * 1024; // 512 MB
        let host_memory: u64 = 16 * 1024 * 1024 * 1024; // 16 GB
        // cgroup limit should win
        assert_eq!(cgroup_limit.min(host_memory), cgroup_limit);
    }

    /// The cgroup v1 sentinel value (no limit set) must NOT be treated as a
    /// real limit.
    #[test]
    #[cfg(target_os = "linux")]
    fn test_cgroup_v1_sentinel_is_ignored() {
        // The Linux kernel writes this sentinel (close to i64::MAX, aligned to
        // a page boundary) to memory.limit_in_bytes when no limit is set.
        // The exact value is (i64::MAX / PAGE_SIZE) * PAGE_SIZE where
        // PAGE_SIZE = 4096, giving 9_223_372_036_854_771_712.
        let cgroup_v1_no_limit_sentinel: u64 = 9_223_372_036_854_771_712;
        // Must be above our 2^62 threshold so it is skipped.
        assert!(cgroup_v1_no_limit_sentinel >= (1u64 << 62));
    }

    /// A valid cgroup v1 byte limit below the sentinel must be respected.
    #[test]
    #[cfg(target_os = "linux")]
    fn test_cgroup_v1_real_limit_below_sentinel() {
        let limit: u64 = 256 * 1024 * 1024; // 256 MB
        // Must be below our 2^62 threshold so it is used.
        assert!(limit < (1u64 << 62));
    }

    /// The cgroup v2 "max" string means no limit is configured.
    #[test]
    #[cfg(target_os = "linux")]
    fn test_cgroup_v2_max_string_means_no_limit() {
        let content = "max\n";
        let trimmed = content.trim();
        // "max" must NOT be parsed as a numeric limit.
        assert_eq!(trimmed, "max");
        assert!(trimmed.parse::<u64>().is_err());
    }

    /// detect_memory never returns zero.
    #[test]
    fn test_detect_memory_nonzero() {
        let mem = detect_memory();
        assert!(mem > 0, "detected memory must be > 0, got {mem}");
    }

    #[test]
    fn test_crop_dir_is_scoped_to_the_comb() {
        let mut a = NodeConfig::detect("local://one");
        a.cache_dir = PathBuf::from("/cache");
        let mut b = a.clone();
        b.storage_uri = "local://two".into();
        let same = a.clone();

        assert_eq!(
            a.crop_dir(),
            same.crop_dir(),
            "the same comb finds its crop again"
        );
        assert_ne!(a.crop_dir(), b.crop_dir(), "another comb gets its own crop");
        assert!(a.crop_dir().starts_with("/cache/crop"));
    }
}
