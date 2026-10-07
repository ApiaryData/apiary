//! Where the entrance puts what it would not admit.
//!
//! A stream (an MQTT topic, say) has no caller to tell that a message was
//! refused, and dropping it silently would lose data. So a Guard sets it aside:
//! the payload goes to a file, with a sidecar saying why, where an operator can
//! inspect it, fix the Frame or the sender, and replay it.

use std::fs;
use std::path::{Path, PathBuf};

use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use apiary_core::{ApiaryError, Result};

/// Why a deposit was set aside, and where it came from.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetAsideRecord {
    /// The Frame the deposit was meant for, as `hive.box.frame`.
    pub frame: String,
    /// The source: an MQTT topic, a Flight client, and so on.
    pub source: String,
    /// Why the Guard refused it.
    pub reason: String,
    /// Rows in the deposit (0 if it never parsed into rows).
    pub rows: u64,
    /// When it was set aside, in milliseconds since the epoch.
    pub at_ms: i64,
    /// The file holding the payload, relative to the set-aside directory.
    pub payload: String,
}

/// A directory of set-aside deposits.
#[derive(Clone, Debug)]
pub struct SetAside {
    dir: PathBuf,
}

impl SetAside {
    /// Open (creating if need be) a set-aside directory.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|e| {
            ApiaryError::storage(format!("Failed to create set-aside directory {dir:?}"), e)
        })?;
        Ok(Self { dir })
    }

    /// The directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Set aside a deposit that parsed into a batch.
    pub fn put_batch(
        &self,
        frame: &str,
        source: &str,
        reason: &str,
        batch: &RecordBatch,
    ) -> Result<SetAsideRecord> {
        let mut bytes = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut bytes, &batch.schema())
                .map_err(|e| internal(format!("Failed to encode a set-aside batch: {e}")))?;
            writer
                .write(batch)
                .and_then(|()| writer.finish())
                .map_err(|e| internal(format!("Failed to encode a set-aside batch: {e}")))?;
        }
        self.put(
            frame,
            source,
            reason,
            batch.num_rows() as u64,
            "arrow",
            &bytes,
        )
    }

    /// Set aside a payload that never parsed into rows.
    pub fn put_raw(
        &self,
        frame: &str,
        source: &str,
        reason: &str,
        payload: &[u8],
    ) -> Result<SetAsideRecord> {
        self.put(frame, source, reason, 0, "raw", payload)
    }

    fn put(
        &self,
        frame: &str,
        source: &str,
        reason: &str,
        rows: u64,
        extension: &str,
        payload: &[u8],
    ) -> Result<SetAsideRecord> {
        let at_ms = chrono::Utc::now().timestamp_millis();
        let id = format!("{at_ms:016}-{}", uuid::Uuid::new_v4().simple());
        let payload_name = format!("{id}.{extension}");
        let record = SetAsideRecord {
            frame: frame.to_string(),
            source: source.to_string(),
            reason: reason.to_string(),
            rows,
            at_ms,
            payload: payload_name.clone(),
        };

        // Payload first, then the sidecar that makes it visible to `list`.
        write(&self.dir.join(&payload_name), payload)?;
        let sidecar = serde_json::to_vec_pretty(&record)
            .map_err(|e| internal(format!("Failed to encode a set-aside record: {e}")))?;
        write(&self.dir.join(format!("{id}.json")), &sidecar)?;
        Ok(record)
    }

    /// Everything set aside, oldest first.
    pub fn list(&self) -> Result<Vec<SetAsideRecord>> {
        let mut records = Vec::new();
        let entries = fs::read_dir(&self.dir).map_err(|e| {
            ApiaryError::storage(format!("Failed to list set-aside {:?}", self.dir), e)
        })?;
        for entry in entries {
            let path = entry
                .map_err(|e| ApiaryError::storage("Failed to list set-aside", e))?
                .path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(&path)
                .map_err(|e| ApiaryError::storage(format!("Failed to read {path:?}"), e))?;
            if let Ok(record) = serde_json::from_slice::<SetAsideRecord>(&bytes) {
                records.push(record);
            }
        }
        records.sort_by(|a, b| a.at_ms.cmp(&b.at_ms).then(a.payload.cmp(&b.payload)));
        Ok(records)
    }

    /// The bytes of a set-aside payload.
    pub fn payload(&self, record: &SetAsideRecord) -> Result<Vec<u8>> {
        let path = self.dir.join(&record.payload);
        fs::read(&path).map_err(|e| ApiaryError::storage(format!("Failed to read {path:?}"), e))
    }
}

fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes).map_err(|e| ApiaryError::storage(format!("Failed to write {path:?}"), e))
}

fn internal(message: String) -> ApiaryError {
    ApiaryError::Internal { message }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;

    use super::*;

    #[test]
    fn put_and_list_round_trip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let aside = SetAside::open(tmp.path().join("aside")).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "n",
            Arc::new(Int64Array::from(vec![1, 2])) as arrow::array::ArrayRef,
        )])
        .unwrap();

        let a = aside
            .put_batch("h.b.f", "client 1", "unknown column 'x'", &batch)
            .unwrap();
        let b = aside
            .put_raw("h.b.f", "topic t", "not JSON", b"{oops")
            .unwrap();

        let listed = aside.list().unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0], a);
        assert_eq!(listed[1], b);
        assert_eq!(a.rows, 2);
        assert_eq!(aside.payload(&b).unwrap(), b"{oops");
        assert!(!aside.payload(&a).unwrap().is_empty());
    }
}
