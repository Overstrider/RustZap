use std::time::Duration;

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

use crate::config::R2Config;

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

    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("R2 upload returned {}", response.status()))
    }
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
