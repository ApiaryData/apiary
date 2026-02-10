//! Local cell cache with LRU eviction.
//!
//! Each node maintains a local cache of recently accessed cells from object storage.
//! The cache uses an LRU (Least Recently Used) eviction policy to stay within its
//! size limit. Cache hits eliminate S3 fetches, significantly improving query performance.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use apiary_core::error::ApiaryError;
use apiary_core::storage::StorageBackend;
use apiary_core::Result;

/// A single entry in the cell cache.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Storage key for this cell (e.g., "cells/frame_id/cell_abc123.parquet").
    pub storage_key: String,

    /// Local filesystem path where the cell is cached.
    pub local_path: PathBuf,

    /// Size of the cached file in bytes.
    pub size: u64,

    /// Last time this entry was accessed (for LRU eviction).
    pub last_accessed: Instant,
}

/// Local cell cache with LRU eviction policy.
///
/// The cache stores recently accessed cells from object storage in the local
/// filesystem. When the cache exceeds its size limit, the least recently
/// accessed entries are evicted.
///
/// # Thread Safety
///
/// The cache is designed for concurrent access from multiple bees. All
/// operations use interior mutability via `RwLock` and atomic operations.
pub struct CellCache {
    /// Directory where cached cells are stored.
    cache_dir: PathBuf,

    /// Maximum total size of the cache in bytes.
    max_size: u64,

    /// Current total size of cached files in bytes (atomic for fast reads).
    current_size: Arc<AtomicU64>,

    /// Map of storage keys to cache entries.
    entries: Arc<RwLock<HashMap<String, CacheEntry>>>,

    /// Reference to the storage backend for fetching cells on cache miss.
    storage: Arc<dyn StorageBackend>,
}

impl CellCache {
    /// Create a new `CellCache` with the specified directory and size limit.
    ///
    /// # Arguments
    ///
    /// * `cache_dir` — Directory where cached cells will be stored.
    /// * `max_size` — Maximum total size of cached files in bytes.
    /// * `storage` — Storage backend for fetching cells on cache miss.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache directory cannot be created.
    pub async fn new(
        cache_dir: PathBuf,
        max_size: u64,
        storage: Arc<dyn StorageBackend>,
    ) -> Result<Self> {
        // Create the cache directory if it doesn't exist
        fs::create_dir_all(&cache_dir)
            .await
            .map_err(|e| ApiaryError::Storage {
                message: format!("Failed to create cache directory: {:?}", cache_dir),
                source: Some(Box::new(e)),
            })?;

        info!(
            cache_dir = ?cache_dir,
            max_size_mb = max_size / (1024 * 1024),
            "Cell cache initialized"
        );

        Ok(Self {
            cache_dir,
            max_size,
            current_size: Arc::new(AtomicU64::new(0)),
            entries: Arc::new(RwLock::new(HashMap::new())),
            storage,
        })
    }

    /// Get a cell from the cache or fetch it from storage on cache miss.
    ///
    /// # Arguments
    ///
    /// * `storage_key` — The storage key for the cell (e.g., "cells/frame_id/cell_abc.parquet").
    ///
    /// # Returns
    ///
    /// Returns the local filesystem path to the cached cell.
    ///
    /// # Behavior
    ///
    /// - **Cache hit**: Updates last accessed time and returns the local path.
    /// - **Cache miss**: Fetches the cell from storage, adds it to the cache, and returns the path.
    /// - Automatically evicts LRU entries if the cache exceeds its size limit.
    pub async fn get(&self, storage_key: &str) -> Result<PathBuf> {
        // Check for cache hit with read lock first
        {
            let entries = self.entries.read().await;
            if let Some(entry) = entries.get(storage_key) {
                let path = entry.local_path.clone();
                drop(entries); // Release read lock before acquiring write lock

                // Update last accessed time with write lock
                let mut entries_write = self.entries.write().await;
                if let Some(entry) = entries_write.get_mut(storage_key) {
                    entry.last_accessed = Instant::now();
                }

                debug!(storage_key, "Cache hit");
                return Ok(path);
            }
        }

        // Cache miss - fetch from storage
        debug!(storage_key, "Cache miss - fetching from storage");

        // Create a local path based on the storage key (sanitize for filesystem)
        let sanitized = storage_key.replace('/', "_");
        let local_path = self.cache_dir.join(&sanitized);

        // Fetch the cell from storage
        let data = self.storage.get(storage_key).await?;
        let size = data.len() as u64;

        // Write to local cache
        let mut file = fs::File::create(&local_path)
            .await
            .map_err(|e| ApiaryError::Storage {
                message: format!("Failed to create cache file: {:?}", local_path),
                source: Some(Box::new(e)),
            })?;

        file.write_all(&data)
            .await
            .map_err(|e| ApiaryError::Storage {
                message: format!("Failed to write cache file: {:?}", local_path),
                source: Some(Box::new(e)),
            })?;

        // Add to cache entries
        {
            let mut entries = self.entries.write().await;
            entries.insert(
                storage_key.to_string(),
                CacheEntry {
                    storage_key: storage_key.to_string(),
                    local_path: local_path.clone(),
                    size,
                    last_accessed: Instant::now(),
                },
            );
        }

        // Update current size
        self.current_size.fetch_add(size, Ordering::SeqCst);

        // Evict if needed
        self.evict_if_needed().await?;

        debug!(storage_key, size, "Cell cached");
        Ok(local_path)
    }

    /// Evict the least recently used entries until the cache is under its size limit.
    ///
    /// This method is called automatically after adding a new entry to the cache.
    ///
    /// Note: Current implementation clones and sorts all entries on each eviction.
    /// This is acceptable for v1 since:
    /// - Evictions are rare (only when cache exceeds limit)
    /// - Typical cache sizes are manageable (hundreds of cells)
    ///
    /// Future optimization: Use a linked hashmap or priority queue for O(1) LRU tracking
    async fn evict_if_needed(&self) -> Result<()> {
        let current = self.current_size.load(Ordering::SeqCst);
        if current <= self.max_size {
            return Ok(());
        }

        info!(
            current_mb = current / (1024 * 1024),
            max_mb = self.max_size / (1024 * 1024),
            "Cache size exceeded - starting LRU eviction"
        );

        let mut entries = self.entries.write().await;

        // Sort entries by last accessed time (oldest first)
        // Note: This clones entries for sorting - acceptable for v1 scale
        let mut sorted: Vec<_> = entries.values().cloned().collect();
        sorted.sort_by_key(|e| e.last_accessed);

        // Evict entries until we're under the limit
        let mut freed = 0u64;
        for entry in sorted {
            if self.current_size.load(Ordering::SeqCst) <= self.max_size {
                break;
            }

            // Remove from entries map
            entries.remove(&entry.storage_key);

            // Delete the local file
            if let Err(e) = fs::remove_file(&entry.local_path).await {
                warn!(
                    path = ?entry.local_path,
                    error = %e,
                    "Failed to delete evicted cache file"
                );
            } else {
                freed += entry.size;
                self.current_size.fetch_sub(entry.size, Ordering::SeqCst);
                debug!(
                    storage_key = entry.storage_key,
                    size = entry.size,
                    "Evicted cache entry"
                );
            }
        }

        info!(
            freed_mb = freed / (1024 * 1024),
            remaining_mb = self.current_size.load(Ordering::SeqCst) / (1024 * 1024),
            "Cache eviction complete"
        );

        Ok(())
    }

    /// Get the current size of the cache in bytes.
    pub fn size(&self) -> u64 {
        self.current_size.load(Ordering::SeqCst)
    }

    /// Get a list of all cached cells for heartbeat reporting.
    ///
    /// Returns a map of storage keys to their sizes in bytes.
    pub async fn list_cached_cells(&self) -> HashMap<String, u64> {
        let entries = self.entries.read().await;
        entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.size))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apiary_storage::local::LocalBackend;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_cache_miss_fetches_from_storage() {
        let storage_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let storage = Arc::new(LocalBackend::new(storage_dir.path()).await.unwrap());

        // Write a test cell to storage
        let test_data = b"test cell data";
        storage
            .put("cells/test.parquet", test_data.as_slice().into())
            .await
            .unwrap();

        // Create cache
        let cache = CellCache::new(
            cache_dir.path().to_path_buf(),
            10 * 1024 * 1024, // 10 MB
            storage.clone(),
        )
        .await
        .unwrap();

        // First access should be a cache miss
        let path = cache.get("cells/test.parquet").await.unwrap();
        assert!(path.exists());

        // Read the cached file
        let cached_data = fs::read(&path).await.unwrap();
        assert_eq!(cached_data, test_data);

        // Check cache size
        assert_eq!(cache.size(), test_data.len() as u64);
    }

    #[tokio::test]
    async fn test_cache_hit_returns_cached_path() {
        let storage_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let storage = Arc::new(LocalBackend::new(storage_dir.path()).await.unwrap());
        storage
            .put("cells/test.parquet", b"data".as_slice().into())
            .await
            .unwrap();

        let cache = CellCache::new(
            cache_dir.path().to_path_buf(),
            10 * 1024 * 1024,
            storage.clone(),
        )
        .await
        .unwrap();

        // First access (miss)
        let path1 = cache.get("cells/test.parquet").await.unwrap();

        // Second access (hit) - should return the same path
        let path2 = cache.get("cells/test.parquet").await.unwrap();
        assert_eq!(path1, path2);
    }

    #[tokio::test]
    async fn test_lru_eviction() {
        let storage_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let storage = Arc::new(LocalBackend::new(storage_dir.path()).await.unwrap());

        // Create test cells
        let data1 = vec![1u8; 1024]; // 1 KB
        let data2 = vec![2u8; 1024]; // 1 KB
        let data3 = vec![3u8; 1024]; // 1 KB

        storage
            .put("cells/cell1.parquet", data1.into())
            .await
            .unwrap();
        storage
            .put("cells/cell2.parquet", data2.into())
            .await
            .unwrap();
        storage
            .put("cells/cell3.parquet", data3.into())
            .await
            .unwrap();

        // Create cache with 2.5 KB limit (can hold 2 cells, will evict on 3rd)
        let cache = CellCache::new(
            cache_dir.path().to_path_buf(),
            2500, // 2.5 KB
            storage.clone(),
        )
        .await
        .unwrap();

        // Cache cell1
        let path1 = cache.get("cells/cell1.parquet").await.unwrap();
        assert!(path1.exists());

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Cache cell2
        let path2 = cache.get("cells/cell2.parquet").await.unwrap();
        assert!(path2.exists());

        // Small delay
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Cache cell3 - should trigger eviction of cell1 (oldest)
        let path3 = cache.get("cells/cell3.parquet").await.unwrap();
        assert!(path3.exists());

        // cell1 should be evicted (file deleted)
        assert!(!path1.exists());

        // cell2 and cell3 should still be cached
        assert!(path2.exists());
        assert!(path3.exists());

        // Cache size should be under limit
        assert!(cache.size() <= 2500);
    }

    #[tokio::test]
    async fn test_list_cached_cells() {
        let storage_dir = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let storage = Arc::new(LocalBackend::new(storage_dir.path()).await.unwrap());
        storage
            .put("cells/cell1.parquet", b"data1".as_slice().into())
            .await
            .unwrap();
        storage
            .put("cells/cell2.parquet", b"data22".as_slice().into())
            .await
            .unwrap();

        let cache = CellCache::new(
            cache_dir.path().to_path_buf(),
            10 * 1024 * 1024,
            storage.clone(),
        )
        .await
        .unwrap();

        // Cache two cells
        cache.get("cells/cell1.parquet").await.unwrap();
        cache.get("cells/cell2.parquet").await.unwrap();

        // List should return both
        let cached = cache.list_cached_cells().await;
        assert_eq!(cached.len(), 2);
        assert_eq!(cached.get("cells/cell1.parquet"), Some(&5u64));
        assert_eq!(cached.get("cells/cell2.parquet"), Some(&6u64));
    }
}
