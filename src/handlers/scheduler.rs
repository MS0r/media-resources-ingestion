use axum::http::HeaderMap;
use futures_util::StreamExt;
use reqwest::header;
use tokio::fs::File;
use sha2::{Sha256, Digest};
use tokio::io::AsyncWriteExt;
use std::{io::Error, path::PathBuf};
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

pub async fn download_file(resource : &Resource) -> Result<(), Box<dyn std::error::Error>> {
    let url = &resource.url;

    let response = reqwest::get(url.as_str()).await?;
        if !response.status().is_success() {
        return Err(format!("Failed to download file: HTTP {}", response.status()).into());
    }

    // let headers = response.headers();
    let filename = filename_from_url(url).unwrap_or_else(|| "downloaded_file".to_string());

    let path = expand_path(&resource.dest.clone().unwrap().path.unwrap(), &filename);
    println!("Downloading from URL: {}", url);

    let mut file = File::create(&path).await?;
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    
    Ok(())
}