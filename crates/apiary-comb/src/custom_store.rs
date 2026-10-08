//! Comb stores that are not a directory or an S3 prefix.
//!
//! A Node that does not have the site's drive plugged in reaches it through the
//! comb host, over the colony's connections. That store is an [`ObjectStore`]
//! built at run time, so it cannot be named by a URL alone. This module gives it
//! a name: register the store under an authority, and the comb root
//! `apiary-drive://<authority>/` resolves to it, for `delta-rs` and for the query
//! engine alike. Everything built on Delta tables (creating, opening and
//! scanning Frames, capping, harvest) then works over it unchanged.
//!
//! [`ObjectStoreBackend`] adapts the same store to [`StorageBackend`], for the
//! registry and the other small files kept beside the tables.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use deltalake::DeltaResult;
use deltalake::DeltaTableError;
use deltalake::logstore::{
    LogStore, LogStoreFactory, ObjectStoreFactory, ObjectStoreRef, StorageConfig, default_logstore,
    logstore_factories, object_store_factories,
};
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use url::Url;

use apiary_core::{ApiaryError, Result, StorageBackend};

/// The URL scheme of a comb reached through a registered store.
pub const DRIVE_SCHEME: &str = "apiary-drive";

fn stores() -> &'static Mutex<HashMap<String, Arc<dyn ObjectStore>>> {
    static STORES: OnceLock<Mutex<HashMap<String, Arc<dyn ObjectStore>>>> = OnceLock::new();
    STORES.get_or_init(Mutex::default)
}

/// Make `store` the comb for `apiary-drive://<authority>/`.
pub fn register_store(authority: &str, store: Arc<dyn ObjectStore>) {
    install_factories();
    stores()
        .lock()
        .expect("store registry poisoned")
        .insert(authority.to_string(), store);
}

/// Forget a registered store.
pub fn unregister_store(authority: &str) {
    stores()
        .lock()
        .expect("store registry poisoned")
        .remove(authority);
}

/// The store registered for an authority, if any.
pub fn lookup_store(authority: &str) -> Option<Arc<dyn ObjectStore>> {
    stores()
        .lock()
        .expect("store registry poisoned")
        .get(authority)
        .cloned()
}

/// The authority of an `apiary-drive://` URI, if it is one.
pub fn drive_authority(uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("apiary-drive://")?;
    let authority = rest.split('/').next().unwrap_or_default();
    (!authority.is_empty()).then(|| authority.to_string())
}

fn install_factories() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let scheme = Url::parse(&format!("{DRIVE_SCHEME}://")).expect("a valid scheme URL");
        object_store_factories().insert(scheme.clone(), Arc::new(DriveObjectStoreFactory));
        logstore_factories().insert(scheme, Arc::new(DriveLogStoreFactory));
    });
}

struct DriveObjectStoreFactory;

impl ObjectStoreFactory for DriveObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        _config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, ObjectPath)> {
        let authority = url.host_str().unwrap_or_default();
        let store = lookup_store(authority).ok_or_else(|| {
            DeltaTableError::Generic(format!(
                "No comb store is registered for {DRIVE_SCHEME}://{authority}/"
            ))
        })?;
        Ok((store, ObjectPath::from(url.path())))
    }
}

struct DriveLogStoreFactory;

impl LogStoreFactory for DriveLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        // Commits are a conditional put of the next log entry, which the store
        // (the comb host's file system) provides.
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// A [`StorageBackend`] over any [`ObjectStore`], under an optional key prefix.
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectStoreBackend {
    /// Keys are stored directly in `store`.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            prefix: String::new(),
        }
    }

    /// Keys are stored under `prefix/` in `store`.
    pub fn with_prefix(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: prefix.trim_matches('/').to_string(),
        }
    }

    fn path(&self, key: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(key)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, key))
        }
    }
}

#[async_trait]
impl StorageBackend for ObjectStoreBackend {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        self.store
            .put(&self.path(key), PutPayload::from(data))
            .await
            .map_err(|e| ApiaryError::storage(format!("Store put failed for {key}"), e))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let result = self.store.get(&self.path(key)).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => ApiaryError::NotFound {
                key: key.to_string(),
            },
            other => ApiaryError::storage(format!("Store get failed for {key}"), other),
        })?;
        result
            .bytes()
            .await
            .map_err(|e| ApiaryError::storage(format!("Store read failed for {key}"), e))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut stream = self.store.list(Some(&self.path(prefix)));
        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(|e| ApiaryError::storage(format!("Store list failed for {prefix}"), e))?
        {
            let full = meta.location.to_string();
            let key = if self.prefix.is_empty() {
                full
            } else {
                full.strip_prefix(&format!("{}/", self.prefix))
                    .unwrap_or(&full)
                    .to_string()
            };
            keys.push(key);
        }
        keys.sort();
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(ApiaryError::storage(
                format!("Store delete failed for {key}"),
                e,
            )),
        }
    }

    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> Result<bool> {
        let options = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&self.path(key), PutPayload::from(data), options)
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(e) => Err(ApiaryError::storage(
                format!("Store conditional put failed for {key}"),
                e,
            )),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self.store.head(&self.path(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(ApiaryError::storage(
                format!("Store head failed for {key}"),
                e,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[test]
    fn authorities_are_read_from_drive_uris() {
        assert_eq!(drive_authority("apiary-drive://abc/"), Some("abc".into()));
        assert_eq!(drive_authority("apiary-drive://abc"), Some("abc".into()));
        assert_eq!(drive_authority("apiary-drive:///"), None);
        assert_eq!(drive_authority("s3://bucket"), None);
    }

    #[tokio::test]
    async fn the_backend_stores_lists_and_creates_only_once() {
        let backend = ObjectStoreBackend::with_prefix(Arc::new(InMemory::new()), "site");
        backend.put("a/one", Bytes::from("1")).await.unwrap();
        backend.put("a/two", Bytes::from("2")).await.unwrap();
        assert_eq!(backend.get("a/one").await.unwrap(), Bytes::from("1"));
        assert_eq!(backend.list("a/").await.unwrap(), vec!["a/one", "a/two"]);
        assert!(backend.exists("a/one").await.unwrap());
        assert!(!backend.exists("a/three").await.unwrap());

        assert!(
            backend
                .put_if_not_exists("lock", Bytes::from("x"))
                .await
                .unwrap()
        );
        assert!(
            !backend
                .put_if_not_exists("lock", Bytes::from("y"))
                .await
                .unwrap()
        );
        assert_eq!(backend.get("lock").await.unwrap(), Bytes::from("x"));

        backend.delete("a/one").await.unwrap();
        backend.delete("a/one").await.unwrap();
        assert!(matches!(
            backend.get("a/one").await,
            Err(ApiaryError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn a_frame_table_can_live_in_a_registered_store() {
        use apiary_core::{FieldDef, FrameSchema};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        register_store("test-comb", Arc::clone(&store));
        let comb = crate::Comb::from_storage_uri("apiary-drive://test-comb/").unwrap();
        let schema = FrameSchema {
            fields: vec![FieldDef {
                name: "n".into(),
                data_type: "int64".into(),
                nullable: false,
            }],
        };
        let table = comb
            .create_frame_table("h", "b", "f", &schema, &[])
            .await
            .unwrap();
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "n",
            Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        comb.append(&table, &batch, 1 << 20, crate::CellState::Nectar)
            .await
            .unwrap();

        // The data went to the registered store, under the Frame's path.
        let files: Vec<_> = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .collect();
        assert!(
            files.iter().any(|f| f.starts_with("h/b/f/_delta_log/")),
            "{files:?}"
        );
        assert!(
            files
                .iter()
                .any(|f| f.starts_with("h/b/f/") && f.ends_with(".parquet"))
        );

        // And it reads back through a fresh open, as another Node would.
        let reopened = comb.open_frame_table("h", "b", "f").await.unwrap().unwrap();
        let read = comb.read(&reopened, None).await.unwrap().unwrap();
        assert_eq!(read.num_rows(), 3);
        unregister_store("test-comb");
    }
}
