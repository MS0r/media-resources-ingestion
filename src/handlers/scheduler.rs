use crate::{models::{Metadata, Provider::Local}, services::mongo::{self, UpsertResult}, state::AppState};
use axum::http::HeaderMap;
use futures_util::StreamExt;
use reqwest::header;
use tokio::fs::File;
use sha2::{Sha256, Digest};
use tokio::io::AsyncWriteExt;
use std::{path::PathBuf};
use url::Url;

use crate::models::Resource;

fn expand_path(path: &str, filename: &str) -> PathBuf {
    let expanded_path = shellexpand::tilde(path).to_string();
    PathBuf::from(expanded_path).join(filename)
}

fn filename_from_url(url: &Url) -> Option<String> {
    url.path_segments()
        .and_then(|segments| segments.filter(|s| !s.is_empty()).last())
        .map(|s| s.to_string())
}

fn get_mime_from_filename(filename: &str) -> Option<String> {
    mime_guess::from_path(filename).first_raw().map(|s| s.to_string())
}

fn get_mime_type(headers: &HeaderMap) -> Option<String> {
    if let Some(ct) = headers.get(header::CONTENT_TYPE) {
        if let Ok(ct_str) = ct.to_str() {
            // remove charset if present
            let mime = ct_str.split(';').next()?.trim();
            if !mime.is_empty() {
                return Some(mime.to_string());
            }
        }
    }

    if let Some(cd) = headers.get(header::CONTENT_DISPOSITION) {
        if let Ok(cd_str) = cd.to_str() {
            if let Some(filename_part) = cd_str
                .split(';')
                .find(|part| part.trim().starts_with("filename="))
            {
                let filename = filename_part
                    .trim()
                    .trim_start_matches("filename=")
                    .trim_matches('"');

                if let Some(mime) = mime_guess::from_path(filename).first_raw() {
                    return Some(mime.to_string());
                }
            }
        }
    }
    Some("No Mime type found".to_string())
}

pub async fn download_file(resource : &Resource, state: &AppState) -> Result<String, Box<dyn std::error::Error>> {
    let url = &resource.url;

    let response = reqwest::get(url.as_str()).await?;
        if !response.status().is_success() {
        return Err(format!("Failed to download file: HTTP {}", response.status()).into());
    }

    let content_length = response.content_length().unwrap_or(0);
    // let headers = response.headers();
    let filename = filename_from_url(url).unwrap_or_else(|| "downloaded_file".to_string());
    let dest = resource
        .dest
        .as_ref()
        .and_then(|d| d.path.as_ref())
        .ok_or("Missing destination path")?;
    let provider = resource
        .dest
        .as_ref()
        .and_then(|d| d.provider.as_ref())
        .unwrap_or(&Local);

    let path = expand_path(dest, &filename);
    tracing::info!("Downloading from URL: {}", url);

    let mut file = File::create(&path).await?;
    let mut stream = response.bytes_stream();
    let mut hasher = Sha256::new();
    let mut bytes_written: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        bytes_written += chunk.len() as u64;
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }
    
    file.flush().await?;

    let hash_hex = hex::encode(hasher.finalize());
    let compressed_file_size = bytes_written;

    let metadata = Metadata::new(
        hash_hex.clone(),
        url.clone(),
        provider.clone(),
        path.to_string_lossy().to_string(),
        content_length, // Fallback to 0 if content length is not provided
        Some(compressed_file_size), // Assuming no compression for now
        get_mime_from_filename(&filename).unwrap_or_else(|| "application/octet-stream".to_string()),
    );

   match state.mongo_service.upsert_resource_metadata(&metadata).await? {
        UpsertResult::Inserted => {
            tracing::info!("New file metadata inserted with hash: {}", hash_hex);
            state.mongo_service.save_resource_metadata(&metadata).await?;
            Ok(hash_hex)
        }
        UpsertResult::Duplicate(existing) => {
            // remove the duplicate local file we just wrote
            tracing::info!("Duplicate file detected. Existing hash: {}", existing.file_hash);
            Ok(existing.file_hash)
        }
    }
}