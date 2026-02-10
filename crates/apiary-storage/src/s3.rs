//! S3-compatible object storage backend.
//!
//! [`S3Backend`] implements the [`StorageBackend`] trait using the `object_store`
//! crate, supporting any S3-compatible endpoint: AWS S3, MinIO, GCS (via S3
//! compatibility), Ceph, etc.
//!
//! Conditional writes use `put_opts` with `IfNotExists` mode, which maps to
//! the `If-None-Match: *` HTTP header (available on S3 since 2024).

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use tracing::{debug, instrument};

use apiary_core::error::ApiaryError;
use apiary_core::storage::StorageBackend;
use apiary_core::Result;

/// A [`StorageBackend`] backed by any S3-compatible object storage.
///
/// Configured from a URI like `s3://bucket/prefix?region=eu-west-1`.
/// Uses the `object_store` crate for S3 operations with built-in
/// retry logic and connection pooling.
pub struct S3Backend {
    store: Box<dyn ObjectStore>,
    prefix: String,
}

impl S3Backend {
    /// Create a new `S3Backend` from an S3 URI.
    ///
    /// # URI Format
    ///
    /// `s3://bucket/prefix?region=us-east-1`
    ///
    /// Environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
    /// and `AWS_REGION` are used as fallbacks for credentials and region.
    pub fn new(uri: &str) -> Result<Self> {
        let (bucket, prefix) = parse_s3_uri(uri)?;

        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&bucket);

        // Extract region from query params if present
        if let Some(region) = extract_query_param(uri, "region") {
            builder = builder.with_region(&region);
        }

        // Extract endpoint for MinIO / custom S3-compatible services
        if let Some(endpoint) = extract_query_param(uri, "endpoint") {
            builder = builder.with_endpoint(&endpoint).with_allow_http(true);
        }

        let store = builder.build().map_err(|e| {
            ApiaryError::storage(format!("Failed to create S3 client for {uri}"), e)
        })?;

        debug!(bucket = %bucket, prefix = %prefix, "S3Backend initialised");

        Ok(Self {
            store: Box::new(store),
            prefix,
        })
    }

    /// Build the full object path from a key.
    fn full_path(&self, key: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(key)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, key))
        }
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    #[instrument(skip(self, data), fields(key = %key, size = data.len()))]
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = self.full_path(key);
        self.store
            .put(&path, PutPayload::from(data))
            .await
            .map_err(|e| ApiaryError::storage(format!("S3 put failed for {key}"), e))?;
        Ok(())
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.full_path(key);
        let result = self.store.get(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => ApiaryError::NotFound {
                key: key.to_string(),
            },
            other => ApiaryError::storage(format!("S3 get failed for {key}"), other),
        })?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| ApiaryError::storage(format!("S3 get bytes failed for {key}"), e))?;
        Ok(bytes)
    }

    #[instrument(skip(self), fields(prefix = %prefix))]
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = if self.prefix.is_empty() {
            ObjectPath::from(prefix)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, prefix))
        };

        let mut results = Vec::new();
        let mut stream = self.store.list(Some(&full_prefix));

        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(|e| ApiaryError::storage(format!("S3 list failed for prefix {prefix}"), e))?
        {
            let full_key = meta.location.to_string();
            // Strip the backend prefix to return keys relative to the storage root
            let key = if self.prefix.is_empty() {
                full_key
            } else {
                full_key
                    .strip_prefix(&format!("{}/", self.prefix))
                    .unwrap_or(&full_key)
                    .to_string()
            };
            results.push(key);
        }

        results.sort();
        Ok(results)
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.full_path(key);
        // S3 delete is idempotent — does not error if key is missing
        self.store
            .delete(&path)
            .await
            .map_err(|e| ApiaryError::storage(format!("S3 delete failed for {key}"), e))?;
        Ok(())
    }

    #[instrument(skip(self, data), fields(key = %key, size = data.len()))]
    async fn put_if_not_exists(&self, key: &str, data: Bytes) -> Result<bool> {
        let path = self.full_path(key);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(data), opts)
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            // Some S3-compatible stores return Precondition instead of AlreadyExists
            Err(object_store::Error::Precondition { .. }) => Ok(false),
            Err(e) => Err(ApiaryError::storage(
                format!("S3 conditional put failed for {key}"),
                e,
            )),
        }
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.full_path(key);
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(ApiaryError::storage(format!("S3 head failed for {key}"), e)),
        }
    }
}

/// Parse an S3 URI into (bucket, prefix).
///
/// `s3://bucket/prefix/path` → `("bucket", "prefix/path")`
/// `s3://bucket` → `("bucket", "")`
fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let stripped = uri
        .strip_prefix("s3://")
        .ok_or_else(|| ApiaryError::Config {
            message: format!("S3 URI must start with 's3://': {uri}"),
        })?;

    // Remove query string before parsing path
    let path_part = stripped.split('?').next().unwrap_or(stripped);

    let mut parts = path_part.splitn(2, '/');
    let bucket = parts.next().unwrap_or("").to_string();
    let prefix = parts.next().unwrap_or("").to_string();

    if bucket.is_empty() {
        return Err(ApiaryError::Config {
            message: format!("S3 URI must include a bucket name: {uri}"),
        });
    }

    Ok((bucket, prefix))
}

/// Extract a query parameter value from a URI.
fn extract_query_param(uri: &str, param: &str) -> Option<String> {
    let query = uri.split('?').nth(1)?;
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next()? == param {
            return kv.next().map(|v| v.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_uri_with_prefix() {
        let (bucket, prefix) = parse_s3_uri("s3://my-bucket/apiary/data").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "apiary/data");
    }

    #[test]
    fn test_parse_s3_uri_no_prefix() {
        let (bucket, prefix) = parse_s3_uri("s3://my-bucket").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_s3_uri_with_query() {
        let (bucket, prefix) = parse_s3_uri("s3://my-bucket/prefix?region=eu-west-1").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "prefix");
    }

    #[test]
    fn test_parse_s3_uri_invalid() {
        assert!(parse_s3_uri("http://example.com").is_err());
        assert!(parse_s3_uri("s3://").is_err());
    }

    #[test]
    fn test_extract_query_param() {
        assert_eq!(
            extract_query_param("s3://bucket?region=us-east-1", "region"),
            Some("us-east-1".to_string())
        );
        assert_eq!(
            extract_query_param(
                "s3://bucket?region=us-east-1&endpoint=http://minio:9000",
                "endpoint"
            ),
            Some("http://minio:9000".to_string())
        );
        assert_eq!(
            extract_query_param("s3://bucket?region=us-east-1", "missing"),
            None
        );
        assert_eq!(extract_query_param("s3://bucket", "region"), None);
    }
}
