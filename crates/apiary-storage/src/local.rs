//! Filesystem-backed storage backend for solo mode and local development.
//!
//! [`LocalBackend`] implements the [`StorageBackend`] trait using the local
//! filesystem. Atomic conditional writes use `OpenOptions::create_new(true)`
//! which maps to `O_CREAT | O_EXCL` on POSIX systems.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs;
use tracing::{debug, instrument};

use apiary_core::error::ApiaryError;
use apiary_core::storage::StorageBackend;
use apiary_core::Result;

/// A [`StorageBackend`] backed by the local filesystem.
///
/// All keys are mapped to paths under the configured `base_dir`.
/// Parent directories are created automatically on `put`.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    base_dir: PathBuf,
}

impl LocalBackend {
    /// Create a new `LocalBackend` rooted at the given directory.
    ///
    /// The directory is created if it does not exist.
    pub async fn new(base_dir: impl Into<PathBuf>) -> Result<Self> {
        let base_dir = base_dir.into();
        fs::create_dir_all(&base_dir).await.map_err(|e| {
            ApiaryError::storage(
                format!("Failed to create base directory: {}", base_dir.display()),
                e,
            )
        })?;
        debug!(base_dir = %base_dir.display(), "LocalBackend initialised");
        Ok(Self { base_dir })
    }

    /// Return the full filesystem path for a storage key.
    fn key_to_path(&self, key: &str) -> PathBuf {
        self.base_dir.join(key)
    }

    /// Return the base directory.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

#[async_trait]
impl StorageBackend for LocalBackend {
    #[instrument(skip(self, data), fields(key = %key, size = data.len()))]
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = self.key_to_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                ApiaryError::storage(
                    format!(
                        "Failed to create parent directories for {}",
                        path.display()
                    ),
                    e,
                )
            })?;
        }
        fs::write(&path, &data).await.map_err(|e| {
            ApiaryError::storage(format!("Failed to write {}", path.display()), e)
        })?;
        debug!("Put {} bytes to {}", data.len(), key);
        Ok(())
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.key_to_path(key);
        let data = fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ApiaryError::NotFound {
                    key: key.to_string(),
                }
            } else {
                ApiaryError::storage(format!("Failed to read {}", path.display()), e)
            }
        })?;
        debug!("Get {} bytes from {}", data.len(), key);
        Ok(Bytes::from(data))
    }

    #[instrument(skip(self), fields(prefix = %prefix))]
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let base = &self.base_dir;
        let mut results = Vec::new();
        list_recursive(base, base, prefix, &mut results).await?;
        results.sort();
        debug!("Listed {} keys with prefix '{}'", results.len(), prefix);
        Ok(results)
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.key_to_path(key);
        match fs::remove_file(&path).await {
            Ok(()) => {
                debug!("Deleted {}", key);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("Delete {}: already absent", key);
                Ok(())
            }
            Err(e) => Err(ApiaryError::storage(
                format!("Failed to delete {}", path.display()),
                e,
            )),
        }
    }

    #[instrument(skip(self, data), fields(key = %key, size = data.len()))]
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> Result<bool> {
        let path = self.key_to_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                ApiaryError::storage(
                    format!(
                        "Failed to create parent directories for {}",
                        path.display()
                    ),
                    e,
                )
            })?;
        }

        // Use std::fs::OpenOptions with create_new(true) for atomic creation.
        // This maps to O_CREAT | O_EXCL on POSIX, providing atomicity.
        let path_clone = path.clone();
        let data_clone = data.clone();
        let result = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path_clone)
            {
                Ok(mut file) => {
                    file.write_all(&data_clone)
                        .map_err(|e| ApiaryError::storage("Failed to write file", e))?;
                    Ok(true)
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
                Err(e) => Err(ApiaryError::storage(
                    format!("Failed to create {}", path_clone.display()),
                    e,
                )),
            }
        })
        .await
        .map_err(|e| ApiaryError::Internal {
            message: format!("Blocking task panicked: {e}"),
        })??;

        debug!(
            "put_if_not_exists {} → {}",
            key,
            if result { "created" } else { "already exists" }
        );
        Ok(result)
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.key_to_path(key);
        let exists = path.exists();
        debug!("exists {} → {}", key, exists);
        Ok(exists)
    }
}

/// Recursively list all files under `dir`, producing keys relative to `base`.
async fn list_recursive(
    base: &Path,
    dir: &Path,
    prefix: &str,
    results: &mut Vec<String>,
) -> Result<()> {
    let mut entries = match fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(ApiaryError::storage(
                format!("Failed to read directory {}", dir.display()),
                e,
            ))
        }
    };

    while let Some(entry) = entries.next_entry().await.map_err(|e| {
        ApiaryError::storage(
            format!("Failed to read directory entry in {}", dir.display()),
            e,
        )
    })? {
        let path = entry.path();
        if path.is_dir() {
            Box::pin(list_recursive(base, &path, prefix, results)).await?;
        } else {
            let relative = path
                .strip_prefix(base)
                .map_err(|e| ApiaryError::Internal {
                    message: format!("Path prefix strip failed: {e}"),
                })?;
            // Normalise to forward slashes for cross-platform key consistency
            let key = relative
                .components()
                .map(|c| c.as_os_str().to_string_lossy().to_string())
                .collect::<Vec<_>>()
                .join("/");
            if key.starts_with(prefix) {
                results.push(key);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn test_backend() -> (LocalBackend, TempDir) {
        let tmp = TempDir::new().unwrap();
        let backend = LocalBackend::new(tmp.path()).await.unwrap();
        (backend, tmp)
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let (backend, _tmp) = test_backend().await;
        backend
            .put("test/file.txt", Bytes::from("hello"))
            .await
            .unwrap();
        let data = backend.get("test/file.txt").await.unwrap();
        assert_eq!(data, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (backend, _tmp) = test_backend().await;
        let result = backend.get("nonexistent").await;
        assert!(matches!(result, Err(ApiaryError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_list() {
        let (backend, _tmp) = test_backend().await;
        backend
            .put("prefix/a.txt", Bytes::from("a"))
            .await
            .unwrap();
        backend
            .put("prefix/b.txt", Bytes::from("b"))
            .await
            .unwrap();
        backend
            .put("other/c.txt", Bytes::from("c"))
            .await
            .unwrap();

        let keys = backend.list("prefix/").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"prefix/a.txt".to_string()));
        assert!(keys.contains(&"prefix/b.txt".to_string()));
    }

    #[tokio::test]
    async fn test_list_empty_prefix() {
        let (backend, _tmp) = test_backend().await;
        backend.put("a.txt", Bytes::from("a")).await.unwrap();
        backend.put("b/c.txt", Bytes::from("c")).await.unwrap();

        let keys = backend.list("").await.unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[tokio::test]
    async fn test_delete() {
        let (backend, _tmp) = test_backend().await;
        backend
            .put("to_delete.txt", Bytes::from("data"))
            .await
            .unwrap();
        backend.delete("to_delete.txt").await.unwrap();
        let result = backend.get("to_delete.txt").await;
        assert!(matches!(result, Err(ApiaryError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_delete_nonexistent() {
        let (backend, _tmp) = test_backend().await;
        // Should not error
        backend.delete("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn test_put_if_not_exists_creates() {
        let (backend, _tmp) = test_backend().await;
        let created = backend
            .put_if_not_exists("new_key.txt", Bytes::from("data"))
            .await
            .unwrap();
        assert!(created);
        let data = backend.get("new_key.txt").await.unwrap();
        assert_eq!(data, Bytes::from("data"));
    }

    #[tokio::test]
    async fn test_put_if_not_exists_returns_false_when_exists() {
        let (backend, _tmp) = test_backend().await;
        backend
            .put("existing.txt", Bytes::from("original"))
            .await
            .unwrap();
        let created = backend
            .put_if_not_exists("existing.txt", Bytes::from("new"))
            .await
            .unwrap();
        assert!(!created);
        // Original data should be unchanged
        let data = backend.get("existing.txt").await.unwrap();
        assert_eq!(data, Bytes::from("original"));
    }

    #[tokio::test]
    async fn test_put_if_not_exists_atomic() {
        let (backend, _tmp) = test_backend().await;
        // First call should succeed
        let first = backend
            .put_if_not_exists("race.txt", Bytes::from("first"))
            .await
            .unwrap();
        assert!(first);
        // Second call should fail
        let second = backend
            .put_if_not_exists("race.txt", Bytes::from("second"))
            .await
            .unwrap();
        assert!(!second);
        // Data should be from first write
        let data = backend.get("race.txt").await.unwrap();
        assert_eq!(data, Bytes::from("first"));
    }

    #[tokio::test]
    async fn test_exists() {
        let (backend, _tmp) = test_backend().await;
        assert!(!backend.exists("missing").await.unwrap());
        backend
            .put("present.txt", Bytes::from("data"))
            .await
            .unwrap();
        assert!(backend.exists("present.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_put_creates_parent_dirs() {
        let (backend, _tmp) = test_backend().await;
        backend
            .put("deep/nested/dir/file.txt", Bytes::from("deep"))
            .await
            .unwrap();
        let data = backend.get("deep/nested/dir/file.txt").await.unwrap();
        assert_eq!(data, Bytes::from("deep"));
    }

    #[tokio::test]
    async fn test_put_overwrites() {
        let (backend, _tmp) = test_backend().await;
        backend
            .put("overwrite.txt", Bytes::from("v1"))
            .await
            .unwrap();
        backend
            .put("overwrite.txt", Bytes::from("v2"))
            .await
            .unwrap();
        let data = backend.get("overwrite.txt").await.unwrap();
        assert_eq!(data, Bytes::from("v2"));
    }

    #[tokio::test]
    async fn test_put_if_not_exists_concurrent() {
        use futures::future::join_all;
        
        let (backend, _tmp) = test_backend().await;
        let backend = std::sync::Arc::new(backend);
        
        // Create 10 concurrent tasks all trying to write to the same key
        // Collect futures first, then await them together for maximum concurrency
        let futures: Vec<_> = (0..10)
            .map(|i| {
                let backend_clone = backend.clone();
                let data = format!("writer-{}", i);
                tokio::spawn(async move {
                    backend_clone
                        .put_if_not_exists("concurrent.txt", Bytes::from(data))
                        .await
                })
            })
            .collect();
        
        // Await all tasks concurrently
        let results = join_all(futures).await;
        
        // Extract the actual results, handling any task panics or I/O errors
        let outcomes: Vec<bool> = results
            .into_iter()
            .map(|join_result| {
                join_result
                    .expect("Task should not panic")
                    .expect("Storage operation should not fail unexpectedly")
            })
            .collect();
        
        // Exactly one task should have succeeded
        let success_count = outcomes.iter().filter(|&&r| r).count();
        assert_eq!(
            success_count, 1,
            "Expected exactly 1 successful write, got {}",
            success_count
        );
        
        // The file should exist and contain data from the winning writer
        let data = backend.get("concurrent.txt").await.unwrap();
        assert!(data.starts_with(b"writer-"));
    }
}
