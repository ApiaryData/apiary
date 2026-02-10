//! Registry types for namespace management (Hives, Boxes, Frames).
//!
//! The registry is the catalog of all namespaces: which hives, boxes, and frames exist,
//! and their metadata. It is persisted as versioned JSON files in object storage using
//! conditional writes for serialization.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::{HiveId, BoxId, FrameId};

/// A versioned registry of all hives, boxes, and frames in the system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Registry {
    /// Monotonically increasing version number.
    pub version: u64,
    
    /// All hives in the registry.
    pub hives: HashMap<String, Hive>,
}

impl Registry {
    /// Create a new empty registry at version 1.
    pub fn new() -> Self {
        Self {
            version: 1,
            hives: HashMap::new(),
        }
    }
    
    /// Create a registry with a specific version.
    pub fn with_version(version: u64) -> Self {
        Self {
            version,
            hives: HashMap::new(),
        }
    }
    
    /// Get the next version number.
    pub fn next_version(&self) -> u64 {
        self.version + 1
    }
    
    /// Check if a hive exists.
    pub fn has_hive(&self, hive_name: &str) -> bool {
        self.hives.contains_key(hive_name)
    }
    
    /// Get a hive by name.
    pub fn get_hive(&self, hive_name: &str) -> Option<&Hive> {
        self.hives.get(hive_name)
    }
    
    /// Get a mutable reference to a hive.
    pub fn get_hive_mut(&mut self, hive_name: &str) -> Option<&mut Hive> {
        self.hives.get_mut(hive_name)
    }
    
    /// Check if a box exists within a hive.
    pub fn has_box(&self, hive_name: &str, box_name: &str) -> bool {
        self.hives
            .get(hive_name)
            .map(|h| h.boxes.contains_key(box_name))
            .unwrap_or(false)
    }
    
    /// Check if a frame exists within a box.
    pub fn has_frame(&self, hive_name: &str, box_name: &str, frame_name: &str) -> bool {
        self.hives
            .get(hive_name)
            .and_then(|h| h.boxes.get(box_name))
            .map(|b| b.frames.contains_key(frame_name))
            .unwrap_or(false)
    }
    
    /// Get a frame by its full path.
    pub fn get_frame(&self, hive_name: &str, box_name: &str, frame_name: &str) -> Option<&Frame> {
        self.hives
            .get(hive_name)
            .and_then(|h| h.boxes.get(box_name))
            .and_then(|b| b.frames.get(frame_name))
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

/// A Hive represents a top-level namespace (database).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Hive {
    /// All boxes (schemas) within this hive.
    pub boxes: HashMap<String, Box>,
    
    /// When this hive was created.
    pub created_at: DateTime<Utc>,
    
    /// Custom properties for this hive.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl Hive {
    /// Create a new hive.
    pub fn new() -> Self {
        Self {
            boxes: HashMap::new(),
            created_at: Utc::now(),
            properties: HashMap::new(),
        }
    }
}

/// A Box represents a schema within a hive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Box {
    /// All frames (tables) within this box.
    pub frames: HashMap<String, Frame>,
    
    /// When this box was created.
    pub created_at: DateTime<Utc>,
    
    /// Custom properties for this box.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl Box {
    /// Create a new box.
    pub fn new() -> Self {
        Self {
            frames: HashMap::new(),
            created_at: Utc::now(),
            properties: HashMap::new(),
        }
    }
}

/// A Frame represents a table within a box.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Frame {
    /// Schema of the frame (Arrow schema JSON).
    pub schema: serde_json::Value,
    
    /// Columns to partition by.
    #[serde(default)]
    pub partition_by: Vec<String>,
    
    /// When this frame was created.
    pub created_at: DateTime<Utc>,
    
    /// Maximum number of partitions allowed (default: 10,000).
    #[serde(default = "default_max_partitions")]
    pub max_partitions: u32,
    
    /// Custom properties for this frame.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

fn default_max_partitions() -> u32 {
    10_000
}

impl Frame {
    /// Create a new frame with a schema.
    pub fn new(schema: serde_json::Value) -> Self {
        Self {
            schema,
            partition_by: Vec::new(),
            created_at: Utc::now(),
            max_partitions: default_max_partitions(),
            properties: HashMap::new(),
        }
    }
    
    /// Create a new frame with schema and partitioning.
    pub fn with_partitioning(
        schema: serde_json::Value,
        partition_by: Vec<String>,
    ) -> Self {
        Self {
            schema,
            partition_by,
            created_at: Utc::now(),
            max_partitions: default_max_partitions(),
            properties: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_registry_new() {
        let registry = Registry::new();
        assert_eq!(registry.version, 1);
        assert_eq!(registry.hives.len(), 0);
    }

    #[test]
    fn test_registry_next_version() {
        let registry = Registry::with_version(5);
        assert_eq!(registry.next_version(), 6);
    }

    #[test]
    fn test_registry_has_hive() {
        let mut registry = Registry::new();
        assert!(!registry.has_hive("analytics"));
        
        let hive = Hive::new();
        registry.hives.insert("analytics".to_string(), hive);
        assert!(registry.has_hive("analytics"));
    }

    #[test]
    fn test_registry_has_box() {
        let mut registry = Registry::new();
        let mut hive = Hive::new();
        let box_ = Box::new();
        hive.boxes.insert("sensors".to_string(), box_);
        registry.hives.insert("analytics".to_string(), hive);
        
        assert!(registry.has_box("analytics", "sensors"));
        assert!(!registry.has_box("analytics", "nonexistent"));
        assert!(!registry.has_box("nonexistent", "sensors"));
    }

    #[test]
    fn test_registry_has_frame() {
        let mut registry = Registry::new();
        let mut hive = Hive::new();
        let mut box_ = Box::new();
        let frame = Frame::new(json!({"fields": []}));
        box_.frames.insert("temperature".to_string(), frame);
        hive.boxes.insert("sensors".to_string(), box_);
        registry.hives.insert("analytics".to_string(), hive);
        
        assert!(registry.has_frame("analytics", "sensors", "temperature"));
        assert!(!registry.has_frame("analytics", "sensors", "nonexistent"));
    }

    #[test]
    fn test_registry_get_frame() {
        let mut registry = Registry::new();
        let mut hive = Hive::new();
        let mut box_ = Box::new();
        let schema = json!({"fields": [{"name": "value", "type": "int"}]});
        let frame = Frame::new(schema.clone());
        box_.frames.insert("temperature".to_string(), frame);
        hive.boxes.insert("sensors".to_string(), box_);
        registry.hives.insert("analytics".to_string(), hive);
        
        let frame = registry.get_frame("analytics", "sensors", "temperature");
        assert!(frame.is_some());
        assert_eq!(frame.unwrap().schema, schema);
    }

    #[test]
    fn test_hive_new() {
        let hive = Hive::new();
        assert_eq!(hive.boxes.len(), 0);
        assert_eq!(hive.properties.len(), 0);
    }

    #[test]
    fn test_box_new() {
        let box_ = Box::new();
        assert_eq!(box_.frames.len(), 0);
        assert_eq!(box_.properties.len(), 0);
    }

    #[test]
    fn test_frame_new() {
        let schema = json!({"fields": []});
        let frame = Frame::new(schema.clone());
        assert_eq!(frame.schema, schema);
        assert_eq!(frame.partition_by.len(), 0);
        assert_eq!(frame.max_partitions, 10_000);
    }

    #[test]
    fn test_frame_with_partitioning() {
        let schema = json!({"fields": []});
        let frame = Frame::with_partitioning(
            schema.clone(),
            vec!["region".to_string(), "date".to_string()],
        );
        assert_eq!(frame.partition_by.len(), 2);
        assert_eq!(frame.partition_by[0], "region");
        assert_eq!(frame.partition_by[1], "date");
    }

    #[test]
    fn test_registry_serialization() {
        let registry = Registry::new();
        let json = serde_json::to_string(&registry).unwrap();
        let deserialized: Registry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, registry);
    }

    #[test]
    fn test_hive_serialization() {
        let hive = Hive::new();
        let json = serde_json::to_string(&hive).unwrap();
        let deserialized: Hive = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, hive);
    }

    #[test]
    fn test_frame_serialization() {
        let schema = json!({"fields": [{"name": "temp", "type": "float"}]});
        let frame = Frame::with_partitioning(
            schema.clone(),
            vec!["region".to_string()],
        );
        let json = serde_json::to_string(&frame).unwrap();
        let deserialized: Frame = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, frame);
    }
}
