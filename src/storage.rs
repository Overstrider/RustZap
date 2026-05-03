use std::{
    path::{Component, PathBuf},
    time::Duration,
};

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use uuid::Uuid;

use crate::config::{AppConfig, R2Config, StorageProvider};

fn r2_bucket_and_credentials(config: &R2Config) -> Result<(Bucket, Credentials), String> {
    let endpoint = config
        .endpoint
        .as_ref()
        .expect("R2 endpoint checked by can_upload")
        .parse()
        .map_err(|err| format!("invalid R2 endpoint: {err}"))?;
    let bucket = Bucket::new(
        endpoint,
        UrlStyle::Path,
        config.bucket.clone(),
        config.region.clone(),
    )
    .map_err(|err| format!("invalid R2 bucket config: {err:?}"))?;
    let credentials = Credentials::new(
        config
            .access_key_id
            .clone()
            .expect("R2 access key checked by can_upload"),
        config
            .secret_access_key
            .clone()
            .expect("R2 secret key checked by can_upload"),
    );
    Ok((bucket, credentials))
}

pub async fn upload_r2_bytes(
    config: &R2Config,
    object_key: &str,
    content_type: &str,
    bytes: Vec<u8>,
) -> Result<(), String> {
    if !config.can_upload() {
        return Ok(());
    }

    let (bucket, credentials) = r2_bucket_and_credentials(config)?;
    let mut action = bucket.put_object(Some(&credentials), object_key);
    action.headers_mut().insert("content-type", content_type);
    let signed_url = action.sign(Duration::from_secs(60));
    let response = reqwest::Client::new()
        .put(signed_url)
        .header("content-type", content_type)
        .body(bytes)
        .send()
        .await
        .map_err(|err| format!("R2 upload request failed: {err}"))?;

    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<body unavailable>".to_string());
        let body = body.chars().take(256).collect::<String>();
        Err(format!("R2 upload returned {status}: {body}"))
    }
}

pub async fn download_r2_bytes(config: &R2Config, object_key: &str) -> Result<Vec<u8>, String> {
    if !config.can_upload() {
        return Err("R2 storage is not configured".to_string());
    }

    let (bucket, credentials) = r2_bucket_and_credentials(config)?;
    let action = bucket.get_object(Some(&credentials), object_key);
    let signed_url = action.sign(Duration::from_secs(60));
    let response = reqwest::Client::new()
        .get(signed_url)
        .send()
        .await
        .map_err(|err| format!("R2 download request failed: {err}"))?;

    if !response.status().is_success() {
        return Err(format!("R2 download returned {}", response.status()));
    }
    response
        .bytes()
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|err| format!("R2 download body failed: {err}"))
}

pub async fn copy_r2_object(
    config: &R2Config,
    source_object_key: &str,
    destination_object_key: &str,
) -> Result<(), String> {
    if !config.can_upload() {
        return Ok(());
    }

    let (bucket, credentials) = r2_bucket_and_credentials(config)?;
    let copy_source = format!("/{}/{}", config.bucket, source_object_key);
    let mut action = bucket.put_object(Some(&credentials), destination_object_key);
    action
        .headers_mut()
        .insert("x-amz-copy-source", &copy_source);
    let signed_url = action.sign(Duration::from_secs(60));
    let response = reqwest::Client::new()
        .put(signed_url)
        .header("x-amz-copy-source", copy_source.clone())
        .send()
        .await
        .map_err(|err| format!("R2 copy request failed: {err}"))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("R2 copy returned {}", response.status()))
    }
}

pub async fn delete_r2_object(config: &R2Config, object_key: &str) -> Result<(), String> {
    if !config.can_upload() {
        return Ok(());
    }

    let (bucket, credentials) = r2_bucket_and_credentials(config)?;
    let action = bucket.delete_object(Some(&credentials), object_key);
    let signed_url = action.sign(Duration::from_secs(60));
    let response = reqwest::Client::new()
        .delete(signed_url)
        .send()
        .await
        .map_err(|err| format!("R2 delete request failed: {err}"))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("R2 delete returned {}", response.status()))
    }
}

pub fn presigned_r2_get_url(
    config: &R2Config,
    object_key: &str,
    ttl_seconds: u64,
) -> Result<Option<String>, String> {
    if !config.can_upload() {
        return Ok(config.public_object_url(object_key));
    }

    let (bucket, credentials) = r2_bucket_and_credentials(config)?;
    let action = bucket.get_object(Some(&credentials), object_key);
    Ok(Some(
        action.sign(Duration::from_secs(ttl_seconds)).to_string(),
    ))
}

#[derive(Clone)]
pub struct MediaByteStore {
    provider: StorageProvider,
    local_dir: PathBuf,
    r2: R2Config,
}

impl MediaByteStore {
    pub fn from_config(config: &AppConfig) -> Self {
        Self {
            provider: config.storage_provider,
            local_dir: config.local_storage_dir.clone(),
            r2: config.r2.clone(),
        }
    }

    pub async fn put(
        &self,
        object_key: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> Result<(), String> {
        match self.provider {
            StorageProvider::LocalFs => self.put_local(object_key, &bytes),
            StorageProvider::R2 => {
                if !self.r2.can_upload() {
                    return Err("R2 storage is not configured".to_string());
                }
                upload_r2_bytes(&self.r2, object_key, content_type, bytes).await
            }
        }
    }

    pub fn put_blocking(
        &self,
        object_key: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> Result<(), String> {
        match self.provider {
            StorageProvider::LocalFs => self.put_local(object_key, &bytes),
            StorageProvider::R2 => {
                let store = self.clone();
                let object_key = object_key.to_string();
                let content_type = content_type.to_string();
                run_storage_future_blocking(async move {
                    store.put(&object_key, &content_type, bytes).await
                })
            }
        }
    }

    pub async fn get(&self, object_key: &str) -> Result<Vec<u8>, String> {
        match self.provider {
            StorageProvider::LocalFs => self.get_local(object_key),
            StorageProvider::R2 => download_r2_bytes(&self.r2, object_key).await,
        }
    }

    pub fn get_blocking(&self, object_key: &str) -> Result<Vec<u8>, String> {
        match self.provider {
            StorageProvider::LocalFs => self.get_local(object_key),
            StorageProvider::R2 => {
                let store = self.clone();
                let object_key = object_key.to_string();
                run_storage_future_blocking(async move { store.get(&object_key).await })
            }
        }
    }

    pub async fn delete(&self, object_key: &str) -> Result<(), String> {
        match self.provider {
            StorageProvider::LocalFs => {
                let path = self.local_object_path(object_key)?;
                if path.exists() {
                    std::fs::remove_file(&path)
                        .map_err(|err| format!("local media delete failed: {err}"))?;
                }
                Ok(())
            }
            StorageProvider::R2 => delete_r2_object(&self.r2, object_key).await,
        }
    }

    pub async fn copy(
        &self,
        source_object_key: &str,
        destination_object_key: &str,
        _content_type: &str,
    ) -> Result<(), String> {
        match self.provider {
            StorageProvider::LocalFs => {
                let bytes = self.get_local(source_object_key)?;
                self.put_local(destination_object_key, &bytes)
            }
            StorageProvider::R2 => {
                if !self.r2.can_upload() {
                    return Err("R2 storage is not configured".to_string());
                }
                copy_r2_object(&self.r2, source_object_key, destination_object_key).await
            }
        }
    }

    pub fn delete_blocking(&self, object_key: &str) -> Result<(), String> {
        match self.provider {
            StorageProvider::LocalFs => {
                let path = self.local_object_path(object_key)?;
                if path.exists() {
                    std::fs::remove_file(&path)
                        .map_err(|err| format!("local media delete failed: {err}"))?;
                }
                Ok(())
            }
            StorageProvider::R2 => {
                let store = self.clone();
                let object_key = object_key.to_string();
                run_storage_future_blocking(async move { store.delete(&object_key).await })
            }
        }
    }

    pub async fn ready_check(&self, r2_write_check: bool) -> Result<String, String> {
        match self.provider {
            StorageProvider::LocalFs => {
                std::fs::create_dir_all(&self.local_dir)
                    .map_err(|err| format!("local storage directory not ready: {err}"))?;
                Ok(format!("local_fs ready at {}", self.local_dir.display()))
            }
            StorageProvider::R2 => {
                if !self.r2.can_upload() {
                    return Err("R2 endpoint/access key/secret/bucket missing".to_string());
                }
                if r2_write_check {
                    let object_key = format!(
                        "{}/readiness/{}.txt",
                        self.r2.base_prefix.trim_matches('/'),
                        Uuid::now_v7().simple()
                    );
                    upload_r2_bytes(&self.r2, &object_key, "text/plain", b"ok".to_vec()).await?;
                    delete_r2_object(&self.r2, &object_key).await?;
                    Ok("r2 ready; write/delete check passed".to_string())
                } else {
                    Ok("r2 configuration ready".to_string())
                }
            }
        }
    }

    fn put_local(&self, object_key: &str, bytes: &[u8]) -> Result<(), String> {
        let path = self.local_object_path(object_key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| format!("local media directory create failed: {err}"))?;
        }
        std::fs::write(&path, bytes).map_err(|err| format!("local media write failed: {err}"))
    }

    fn get_local(&self, object_key: &str) -> Result<Vec<u8>, String> {
        let path = self.local_object_path(object_key)?;
        std::fs::read(&path).map_err(|err| format!("local media read failed: {err}"))
    }

    fn local_object_path(&self, object_key: &str) -> Result<PathBuf, String> {
        if object_key.trim().is_empty() {
            return Err("object key is empty".to_string());
        }
        let mut relative = PathBuf::new();
        for component in PathBuf::from(object_key).components() {
            match component {
                Component::Normal(part) => relative.push(part),
                _ => return Err("object key contains unsafe path components".to_string()),
            }
        }
        Ok(self.local_dir.join(relative))
    }
}

fn run_storage_future_blocking<F, T>(future: F) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, String>> + Send + 'static,
    T: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| format!("failed to build storage runtime: {err}"))?
                .block_on(future)
        })
        .join()
        .map_err(|_| "storage runtime thread panicked".to_string())?
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| format!("failed to build storage runtime: {err}"))?
            .block_on(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;

    #[tokio::test]
    async fn local_media_byte_store_round_trips_and_rejects_path_traversal() {
        let mut config = AppConfig::from_env();
        config.storage_provider = StorageProvider::LocalFs;
        config.local_storage_dir = std::env::temp_dir().join(format!(
            "rustzap-media-store-test-{}",
            Uuid::now_v7().simple()
        ));
        let store = MediaByteStore::from_config(&config);

        store
            .put(
                "project=p/company=c/media=test.bin",
                "application/octet-stream",
                b"bytes".to_vec(),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .get("project=p/company=c/media=test.bin")
                .await
                .unwrap(),
            b"bytes"
        );
        store
            .delete("project=p/company=c/media=test.bin")
            .await
            .unwrap();
        assert!(
            store
                .get("project=p/company=c/media=test.bin")
                .await
                .is_err()
        );
        assert!(
            store
                .put("../escape.bin", "application/octet-stream", b"no".to_vec())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn local_media_byte_store_copies_objects() {
        let mut config = AppConfig::from_env();
        config.storage_provider = StorageProvider::LocalFs;
        config.local_storage_dir = std::env::temp_dir().join(format!(
            "rustzap-media-store-copy-test-{}",
            Uuid::now_v7().simple()
        ));
        let store = MediaByteStore::from_config(&config);

        store
            .put(
                "temp/media.bin",
                "application/octet-stream",
                b"bytes".to_vec(),
            )
            .await
            .unwrap();
        store
            .copy(
                "temp/media.bin",
                "permanent/media.bin",
                "application/octet-stream",
            )
            .await
            .unwrap();

        assert_eq!(store.get("permanent/media.bin").await.unwrap(), b"bytes");
    }
}
