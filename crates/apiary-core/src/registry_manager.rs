//! Registry manager for DDL operations and persistence.
//!
//! The registry manager handles creating, reading, and updating the registry
//! using conditional writes for atomic updates.

use crate::{
    error::ApiaryError,
    registry::{Box as ApiaryBox, Frame, Hive, Registry},
    storage::StorageBackend,
    Result,
};
use bytes::Bytes;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Path prefix for registry files in object storage.
const REGISTRY_PREFIX: &str = "_registry";

/// Format for registry state file names.
fn registry_state_key(version: u64) -> String {
    format!("{}/state_{:06}.json", REGISTRY_PREFIX, version)
}

/// Registry manager for DDL operations.
pub struct RegistryManager {
    storage: Arc<dyn StorageBackend>,
}

impl RegistryManager {
    /// Create a new registry manager.
    pub fn new(storage: Arc<dyn StorageBackend>) -> Self {
        Self { storage }
    }

    /// Load the latest registry from storage, or create a new one if none exists.
    pub async fn load_or_create(&self) -> Result<Registry> {
        // List all registry state files
        let keys = self.storage.list(REGISTRY_PREFIX).await?;

        if keys.is_empty() {
            info!("No registry found, creating new registry");
            return self.create_initial_registry().await;
        }

        // Find the latest version
        let mut max_version = 0u64;
        for key in &keys {
            if let Some(version) = self.extract_version(key) {
                max_version = max_version.max(version);
            }
        }

        if max_version == 0 {
            warn!("No valid registry files found, creating new registry");
            return self.create_initial_registry().await;
        }

        info!("Loading registry version {}", max_version);
        self.load_version(max_version).await
    }

    /// Load a specific version of the registry.
    pub async fn load_version(&self, version: u64) -> Result<Registry> {
        let key = registry_state_key(version);
        let data = self.storage.get(&key).await?;
        let registry: Registry = serde_json::from_slice(&data).map_err(|e| {
            ApiaryError::Serialization(format!("Failed to deserialize registry: {}", e))
        })?;

        debug!(
            "Loaded registry version {} with {} hives",
            registry.version,
            registry.hives.len()
        );
        Ok(registry)
    }

    /// Create a hive in the registry.
    pub async fn create_hive(&self, hive_name: &str) -> Result<Registry> {
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 10;

        loop {
            // Load the current registry
            let mut registry = self.load_or_create().await?;

            // Check if hive already exists (idempotency)
            if registry.has_hive(hive_name) {
                info!("Hive '{}' already exists", hive_name);
                return Ok(registry);
            }

            // Add the new hive
            let hive = Hive::new();
            registry.hives.insert(hive_name.to_string(), hive);
            registry.version = registry.next_version();

            // Try to write the new version
            match self.try_commit_registry(&registry).await {
                Ok(true) => {
                    info!(
                        "Created hive '{}' at registry version {}",
                        hive_name, registry.version
                    );
                    return Ok(registry);
                }
                Ok(false) => {
                    // Conflict - another writer won, retry
                    retry_count += 1;
                    if retry_count >= MAX_RETRIES {
                        return Err(ApiaryError::Conflict {
                            message: format!("Failed to create hive after {} retries", MAX_RETRIES),
                        });
                    }
                    warn!(
                        "Conflict creating hive '{}', retrying ({}/{})",
                        hive_name, retry_count, MAX_RETRIES
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Create a box within a hive.
    pub async fn create_box(&self, hive_name: &str, box_name: &str) -> Result<Registry> {
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 10;

        loop {
            // Load the current registry
            let mut registry = self.load_or_create().await?;

            // Check if hive exists
            if !registry.has_hive(hive_name) {
                return Err(ApiaryError::EntityNotFound {
                    entity_type: "Hive".to_string(),
                    name: hive_name.to_string(),
                });
            }

            // Check if box already exists (idempotency)
            if registry.has_box(hive_name, box_name) {
                info!("Box '{}.{}' already exists", hive_name, box_name);
                return Ok(registry);
            }

            // Add the new box
            let box_ = ApiaryBox::new();
            registry
                .get_hive_mut(hive_name)
                .unwrap()
                .boxes
                .insert(box_name.to_string(), box_);
            registry.version = registry.next_version();

            // Try to write the new version
            match self.try_commit_registry(&registry).await {
                Ok(true) => {
                    info!(
                        "Created box '{}.{}' at registry version {}",
                        hive_name, box_name, registry.version
                    );
                    return Ok(registry);
                }
                Ok(false) => {
                    // Conflict - another writer won, retry
                    retry_count += 1;
                    if retry_count >= MAX_RETRIES {
                        return Err(ApiaryError::Conflict {
                            message: format!("Failed to create box after {} retries", MAX_RETRIES),
                        });
                    }
                    warn!(
                        "Conflict creating box '{}.{}', retrying ({}/{})",
                        hive_name, box_name, retry_count, MAX_RETRIES
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Create a frame within a box.
    pub async fn create_frame(
        &self,
        hive_name: &str,
        box_name: &str,
        frame_name: &str,
        schema: serde_json::Value,
        partition_by: Vec<String>,
    ) -> Result<Registry> {
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 10;

        loop {
            // Load the current registry
            let mut registry = self.load_or_create().await?;

            // Check if hive and box exist
            if !registry.has_hive(hive_name) {
                return Err(ApiaryError::EntityNotFound {
                    entity_type: "Hive".to_string(),
                    name: hive_name.to_string(),
                });
            }
            if !registry.has_box(hive_name, box_name) {
                return Err(ApiaryError::EntityNotFound {
                    entity_type: "Box".to_string(),
                    name: format!("{}.{}", hive_name, box_name),
                });
            }

            // Check if frame already exists (idempotency)
            if registry.has_frame(hive_name, box_name, frame_name) {
                info!(
                    "Frame '{}.{}.{}' already exists",
                    hive_name, box_name, frame_name
                );
                return Ok(registry);
            }

            // Add the new frame
            let frame = if partition_by.is_empty() {
                Frame::new(schema.clone())
            } else {
                Frame::with_partitioning(schema.clone(), partition_by.clone())
            };

            registry
                .get_hive_mut(hive_name)
                .unwrap()
                .boxes
                .get_mut(box_name)
                .unwrap()
                .frames
                .insert(frame_name.to_string(), frame);
            registry.version = registry.next_version();

            // Try to write the new version
            match self.try_commit_registry(&registry).await {
                Ok(true) => {
                    info!(
                        "Created frame '{}.{}.{}' at registry version {}",
                        hive_name, box_name, frame_name, registry.version
                    );
                    return Ok(registry);
                }
                Ok(false) => {
                    // Conflict - another writer won, retry
                    retry_count += 1;
                    if retry_count >= MAX_RETRIES {
                        return Err(ApiaryError::Conflict {
                            message: format!(
                                "Failed to create frame after {} retries",
                                MAX_RETRIES
                            ),
                        });
                    }
                    warn!(
                        "Conflict creating frame '{}.{}.{}', retrying ({}/{})",
                        hive_name, box_name, frame_name, retry_count, MAX_RETRIES
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// List all hives.
    pub async fn list_hives(&self) -> Result<Vec<String>> {
        let registry = self.load_or_create().await?;
        let mut hives: Vec<String> = registry.hives.keys().cloned().collect();
        hives.sort();
        Ok(hives)
    }

    /// List all boxes in a hive.
    pub async fn list_boxes(&self, hive_name: &str) -> Result<Vec<String>> {
        let registry = self.load_or_create().await?;

        let hive = registry
            .get_hive(hive_name)
            .ok_or_else(|| ApiaryError::EntityNotFound {
                entity_type: "Hive".to_string(),
                name: hive_name.to_string(),
            })?;

        let mut boxes: Vec<String> = hive.boxes.keys().cloned().collect();
        boxes.sort();
        Ok(boxes)
    }

    /// List all frames in a box.
    pub async fn list_frames(&self, hive_name: &str, box_name: &str) -> Result<Vec<String>> {
        let registry = self.load_or_create().await?;

        let hive = registry
            .get_hive(hive_name)
            .ok_or_else(|| ApiaryError::EntityNotFound {
                entity_type: "Hive".to_string(),
                name: hive_name.to_string(),
            })?;

        let box_ = hive
            .boxes
            .get(box_name)
            .ok_or_else(|| ApiaryError::EntityNotFound {
                entity_type: "Box".to_string(),
                name: format!("{}.{}", hive_name, box_name),
            })?;

        let mut frames: Vec<String> = box_.frames.keys().cloned().collect();
        frames.sort();
        Ok(frames)
    }

    /// Get a frame's metadata.
    pub async fn get_frame(
        &self,
        hive_name: &str,
        box_name: &str,
        frame_name: &str,
    ) -> Result<Frame> {
        let registry = self.load_or_create().await?;

        registry
            .get_frame(hive_name, box_name, frame_name)
            .cloned()
            .ok_or_else(|| ApiaryError::EntityNotFound {
                entity_type: "Frame".to_string(),
                name: format!("{}.{}.{}", hive_name, box_name, frame_name),
            })
    }

    // Private helper methods

    /// Create the initial registry and persist it.
    async fn create_initial_registry(&self) -> Result<Registry> {
        let registry = Registry::new();

        match self.try_commit_registry(&registry).await {
            Ok(true) => {
                info!("Created initial registry version 1");
                Ok(registry)
            }
            Ok(false) => {
                // Someone else created it first, load it
                info!("Initial registry already created by another node");
                self.load_version(1).await
            }
            Err(e) => Err(e),
        }
    }

    /// Try to commit a registry using conditional write.
    /// Returns Ok(true) if successful, Ok(false) if key already exists.
    async fn try_commit_registry(&self, registry: &Registry) -> Result<bool> {
        let key = registry_state_key(registry.version);
        let json = serde_json::to_string_pretty(registry).map_err(|e| {
            ApiaryError::Serialization(format!("Failed to serialize registry: {}", e))
        })?;

        let data = Bytes::from(json);
        self.storage.put_if_not_exists(&key, data).await
    }

    /// Extract version number from a registry state key.
    fn extract_version(&self, key: &str) -> Option<u64> {
        // Expected format: _registry/state_000001.json
        key.strip_prefix(&format!("{}/state_", REGISTRY_PREFIX))?
            .strip_suffix(".json")?
            .parse::<u64>()
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageBackend;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // Simple in-memory storage backend for testing
    struct MemoryBackend {
        data: Mutex<HashMap<String, Bytes>>,
    }

    impl MemoryBackend {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for MemoryBackend {
        async fn put(&self, key: &str, data: Bytes) -> Result<()> {
            self.data.lock().unwrap().insert(key.to_string(), data);
            Ok(())
        }

        async fn get(&self, key: &str) -> Result<Bytes> {
            self.data
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| ApiaryError::NotFound {
                    key: key.to_string(),
                })
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            let data = self.data.lock().unwrap();
            Ok(data
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.data.lock().unwrap().remove(key);
            Ok(())
        }

        async fn put_if_not_exists(&self, key: &str, data: Bytes) -> Result<bool> {
            let mut map = self.data.lock().unwrap();
            if map.contains_key(key) {
                Ok(false)
            } else {
                map.insert(key.to_string(), data);
                Ok(true)
            }
        }

        async fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.data.lock().unwrap().contains_key(key))
        }
    }

    #[tokio::test]
    async fn test_create_initial_registry() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        let registry = manager.load_or_create().await.unwrap();
        assert_eq!(registry.version, 1);
        assert_eq!(registry.hives.len(), 0);
    }

    #[tokio::test]
    async fn test_create_hive() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        let registry = manager.create_hive("analytics").await.unwrap();
        assert_eq!(registry.version, 2);
        assert!(registry.has_hive("analytics"));
    }

    #[tokio::test]
    async fn test_create_hive_idempotent() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        let registry1 = manager.create_hive("analytics").await.unwrap();
        let registry2 = manager.create_hive("analytics").await.unwrap();

        // Should not increment version on second call
        assert_eq!(registry1.version, registry2.version);
    }

    #[tokio::test]
    async fn test_create_box() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        manager.create_hive("analytics").await.unwrap();
        let registry = manager.create_box("analytics", "sensors").await.unwrap();

        assert!(registry.has_box("analytics", "sensors"));
    }

    #[tokio::test]
    async fn test_create_box_hive_not_found() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        let result = manager.create_box("analytics", "sensors").await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ApiaryError::EntityNotFound { .. }
        ));
    }

    #[tokio::test]
    async fn test_create_frame() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        manager.create_hive("analytics").await.unwrap();
        manager.create_box("analytics", "sensors").await.unwrap();

        let schema = serde_json::json!({
            "fields": [
                {"name": "temperature", "type": "float"},
                {"name": "timestamp", "type": "timestamp"}
            ]
        });

        let registry = manager
            .create_frame("analytics", "sensors", "temperature", schema, vec![])
            .await
            .unwrap();

        assert!(registry.has_frame("analytics", "sensors", "temperature"));
    }

    #[tokio::test]
    async fn test_create_frame_with_partitioning() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        manager.create_hive("analytics").await.unwrap();
        manager.create_box("analytics", "sensors").await.unwrap();

        let schema = serde_json::json!({"fields": []});
        let partition_by = vec!["region".to_string(), "date".to_string()];

        manager
            .create_frame(
                "analytics",
                "sensors",
                "temperature",
                schema,
                partition_by.clone(),
            )
            .await
            .unwrap();

        let frame = manager
            .get_frame("analytics", "sensors", "temperature")
            .await
            .unwrap();

        assert_eq!(frame.partition_by, partition_by);
    }

    #[tokio::test]
    async fn test_list_hives() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        manager.create_hive("analytics").await.unwrap();
        manager.create_hive("production").await.unwrap();

        let hives = manager.list_hives().await.unwrap();
        assert_eq!(hives, vec!["analytics", "production"]);
    }

    #[tokio::test]
    async fn test_list_boxes() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        manager.create_hive("analytics").await.unwrap();
        manager.create_box("analytics", "sensors").await.unwrap();
        manager.create_box("analytics", "metrics").await.unwrap();

        let boxes = manager.list_boxes("analytics").await.unwrap();
        assert_eq!(boxes, vec!["metrics", "sensors"]);
    }

    #[tokio::test]
    async fn test_list_frames() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        manager.create_hive("analytics").await.unwrap();
        manager.create_box("analytics", "sensors").await.unwrap();

        let schema = serde_json::json!({"fields": []});
        manager
            .create_frame(
                "analytics",
                "sensors",
                "temperature",
                schema.clone(),
                vec![],
            )
            .await
            .unwrap();
        manager
            .create_frame("analytics", "sensors", "humidity", schema, vec![])
            .await
            .unwrap();

        let frames = manager.list_frames("analytics", "sensors").await.unwrap();
        assert_eq!(frames, vec!["humidity", "temperature"]);
    }

    #[tokio::test]
    async fn test_get_frame() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        manager.create_hive("analytics").await.unwrap();
        manager.create_box("analytics", "sensors").await.unwrap();

        let schema = serde_json::json!({
            "fields": [{"name": "value", "type": "float"}]
        });

        manager
            .create_frame(
                "analytics",
                "sensors",
                "temperature",
                schema.clone(),
                vec![],
            )
            .await
            .unwrap();

        let frame = manager
            .get_frame("analytics", "sensors", "temperature")
            .await
            .unwrap();

        assert_eq!(frame.schema, schema);
    }

    #[tokio::test]
    async fn test_extract_version() {
        let storage = Arc::new(MemoryBackend::new());
        let manager = RegistryManager::new(storage);

        assert_eq!(
            manager.extract_version("_registry/state_000001.json"),
            Some(1)
        );
        assert_eq!(
            manager.extract_version("_registry/state_000042.json"),
            Some(42)
        );
        assert_eq!(
            manager.extract_version("_registry/state_999999.json"),
            Some(999999)
        );
        assert_eq!(manager.extract_version("invalid"), None);
    }
}
