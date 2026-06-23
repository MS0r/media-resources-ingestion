use crate::{
    error::JobError,
    handlers::jobs::{JobEnvelope, types::DownloadInfo},
    models::Resource,
};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use url::Url;
use wreq::{
    Client, RequestBuilder, Response,
    header::{ACCEPT_RANGES, AUTHORIZATION, CONTENT_TYPE, COOKIE, HeaderValue, RANGE, REFERER},
};

use super::{FileJob, expand_path};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filename_from_url_with_extension() {
        let url = Url::parse("https://example.com/path/to/video.mp4").unwrap();
        let (name, ext) = filename_from_url(&url);
        assert_eq!(name, Some("video".to_string()));
        assert_eq!(ext, Some("mp4".to_string()));
    }

    #[test]
    fn test_filename_from_url_no_extension() {
        let url = Url::parse("https://example.com/path/to/README").unwrap();
        let (name, ext) = filename_from_url(&url);
        assert_eq!(name, Some("README".to_string()));
        assert_eq!(ext, None);
    }

    #[test]
    fn test_filename_from_url_trailing_slash() {
        let url = Url::parse("https://example.com/path/to/").unwrap();
        let (name, ext) = filename_from_url(&url);
        assert_eq!(name, Some("to".to_string()));
        assert_eq!(ext, None);
    }

    #[test]
    fn test_filename_from_url_root_path() {
        let url = Url::parse("https://example.com/").unwrap();
        let (name, ext) = filename_from_url(&url);
        assert_eq!(name, None);
        assert_eq!(ext, None);
    }

    #[test]
    fn test_filename_from_url_no_path() {
        let url = Url::parse("https://example.com").unwrap();
        let (name, ext) = filename_from_url(&url);
        assert_eq!(name, None);
        assert_eq!(ext, None);
    }

    #[test]
    fn test_filename_from_url_dotted_filename() {
        let url = Url::parse("https://example.com/.hidden").unwrap();
        let (name, ext) = filename_from_url(&url);
        assert_eq!(name, Some("".to_string()));
        assert_eq!(ext, Some("hidden".to_string()));
    }

    #[test]
    fn test_filename_from_url_multiple_extensions() {
        let url = Url::parse("https://example.com/archive.tar.gz").unwrap();
        let (name, ext) = filename_from_url(&url);
        // last dot is the split point
        assert_eq!(name, Some("archive.tar".to_string()));
        assert_eq!(ext, Some("gz".to_string()));
    }

    #[test]
    fn test_filename_from_url_root_file() {
        let url = Url::parse("https://example.com/file.txt").unwrap();
        let (name, ext) = filename_from_url(&url);
        assert_eq!(name, Some("file".to_string()));
        assert_eq!(ext, Some("txt".to_string()));
    }
}

pub(crate) struct HeadInfo {
    pub content_length: Option<u64>,
    pub accept_ranges: bool,
    pub mime_type: String,
}

pub(crate) enum StreamResult {
    Completed {
        temp_path: String,
        hash: String,
        bytes_mime: String,
        byte_count: u64,
    },
    ThresholdExceeded {
        byte_count: u64,
    },
}

pub(crate) fn filename_from_url(url: &Url) -> (Option<String>, Option<String>) {
    let path = url
        .path_segments()
        .and_then(|segments| segments.filter(|s| !s.is_empty()).next_back())
        .map(|s| s.to_string());

    match path {
        None => (None, None),
        Some(p) => match p.rfind('.') {
            None => (Some(p), None),
            Some(dot_pos) => {
                let name = p[..dot_pos].to_string();
                let ext = p[dot_pos + 1..].to_string();
                (Some(name), Some(ext))
            }
        },
    }
}

pub(crate) fn extract_file_job(job: &JobEnvelope) -> Result<&FileJob, JobError> {
    match job {
        JobEnvelope::File(file_job) => Ok(file_job),
        _ => Err(JobError::OtherFatal(
            "FileJobHandler received non-file job".into(),
        )),
    }
}

/// Apply auth headers to a request builder.
///
/// If `auth_token` is provided (from dynamic source auth resolution), it is
/// used as `Authorization: Bearer <token>`. Otherwise, falls back to the
/// static `authorization` / `cookie` headers from the resource config.
fn apply_auth_headers(
    mut request: RequestBuilder,
    resource: &Resource,
    auth_token: Option<&str>,
) -> RequestBuilder {
    let origin = resource.url.origin();
    if let Ok(val) = HeaderValue::from_str(&origin.ascii_serialization()) {
        request = request.header(REFERER, val);
    }
    if let Some(token) = auth_token {
        request = request.header(AUTHORIZATION, format!("Bearer {}", token));
    } else if let Some(headers) = &resource.config.as_ref().and_then(|c| c.headers.as_ref()) {
        if let Some(auth) = &headers.authorization {
            request = request.header(AUTHORIZATION, auth);
        }
        if let Some(cookie) = &headers.cookie {
            request = request.header(COOKIE, cookie);
        }
    }
    request
}

/// HEAD preflight — get Content-Length and Accept-Ranges without downloading.
pub(crate) async fn initiate_head(
    resource: &Resource,
    client: &Client,
    auth_token: Option<&str>,
) -> Result<HeadInfo, JobError> {
    let request = apply_auth_headers(client.head(resource.url.as_str()), resource, auth_token);
    let response = request.send().await?;

    if !response.status().is_success() {
        return Err(JobError::OtherFatal(format!(
            "HEAD request failed: HTTP {}",
            response.status()
        )));
    }

    let content_length = response.content_length();
    let accept_ranges = response
        .headers()
        .get(ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map_or(false, |v| v.contains("bytes"));

    let mime_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .map_or("application/octet-stream".to_string(), |s| {
            s.trim().to_string()
        });

    Ok(HeadInfo {
        content_length,
        accept_ranges,
        mime_type,
    })
}

/// GET with `Range: bytes=start-end`.
pub(crate) async fn initiate_range_download(
    url: &Url,
    offset_start: u64,
    offset_end: u64,
    authorization: Option<&str>,
    cookie: Option<&str>,
    client: &Client,
) -> Result<(Response, String), JobError> {
    let range_val = format!("bytes={}-{}", offset_start, offset_end);
    let mut request = client.get(url.as_str()).header(RANGE, &range_val);

    let origin = url.origin();
    if let Ok(val) = HeaderValue::from_str(&origin.ascii_serialization()) {
        request = request.header(REFERER, val);
    }
    if let Some(auth) = authorization {
        request = request.header(AUTHORIZATION, auth);
    }
    if let Some(cookie) = cookie {
        request = request.header(COOKIE, cookie);
    }

    let response = request.send().await?;

    if !response.status().is_success() && response.status() != 206 {
        return Err(JobError::OtherFatal(format!(
            "Range request failed: HTTP {}",
            response.status()
        )));
    }

    let mime_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .map_or("application/octet-stream".to_string(), |s| {
            s.trim().to_string()
        });

    Ok((response, mime_type))
}

/// Stream response body to a temp file, hashing as we go.
/// If `max_bytes` is set and the stream exceeds it, the partial file is
/// deleted and `ThresholdExceeded` is returned.
pub(crate) async fn download_to_temp(
    response: Response,
    temp_dir: &str,
    filename: &str,
    job_id: &str,
    max_bytes: Option<u64>,
) -> Result<StreamResult, JobError> {
    let temp_path = expand_path(temp_dir, &format!("{}.tmp_{}", filename, job_id))
        .to_string_lossy()
        .to_string();
    let mut file = tokio::fs::File::create(&temp_path).await?;
    let mut stream = response.bytes_stream();
    let mut hasher = Sha256::new();
    let mut header_buf = Vec::with_capacity(3072);
    let mut byte_count: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let len = chunk.len() as u64;
        byte_count += len;

        if let Some(limit) = max_bytes {
            if byte_count > limit {
                drop(stream);
                drop(file);
                tokio::fs::remove_file(&temp_path).await.ok();
                tracing::info!(
                    job_id = %job_id,
                    "Download aborted at {} bytes (threshold {})",
                    byte_count, limit
                );
                return Ok(StreamResult::ThresholdExceeded { byte_count });
            }
        }

        hasher.update(&chunk);
        file.write_all(&chunk).await?;
        if header_buf.len() < 3072 {
            let remaining = 3072 - header_buf.len();
            let end = remaining.min(chunk.len());
            header_buf.extend_from_slice(&chunk[..end]);
        }
    }

    tracing::info!(job_id = %job_id, "Download complete, {} bytes at {}", byte_count, temp_path);
    let hash = hex::encode(hasher.finalize());
    let bytes_mime = mimetype_detector::detect(&header_buf).mime().to_string();

    Ok(StreamResult::Completed {
        temp_path,
        hash,
        bytes_mime,
        byte_count,
    })
}

/// Download a byte-range response (chunk) and return (temp_path, sha256, byte_count).
pub(crate) async fn download_range_chunk(
    response: Response,
    temp_dir: &str,
    chunk_label: &str,
    job_id: &str,
) -> Result<(String, String, u64), JobError> {
    let temp_path = expand_path(temp_dir, &format!("{}.tmp_{}", chunk_label, job_id))
        .to_string_lossy()
        .to_string();
    let mut file = tokio::fs::File::create(&temp_path).await?;
    let mut stream = response.bytes_stream();
    let mut hasher = Sha256::new();
    let mut byte_count: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let len = chunk.len() as u64;
        byte_count += len;
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }

    let hash = hex::encode(hasher.finalize());
    tracing::info!(
        job_id = %job_id, chunk = %chunk_label,
        "Chunk download complete, {} bytes at {}", byte_count, temp_path
    );

    Ok((temp_path, hash, byte_count))
}

pub(crate) async fn initiate_download(
    resource: &Resource,
    client: &Client,
    auth_token: Option<&str>,
) -> Result<(Response, DownloadInfo), JobError> {
    let request = apply_auth_headers(client.get(resource.url.as_str()), resource, auth_token);
    let response = request.send().await?;

    if !response.status().is_success() {
        return Err(JobError::OtherFatal(format!(
            "Failed to download file: HTTP {}",
            response.status()
        )));
    }

    tracing::debug!("status = {}", response.status());
    tracing::debug!("version = {:?}", response.version());

    for (k, v) in response.headers() {
        tracing::debug!("{} = {:?}", k, v);
    }

    let content_length = response.content_length().unwrap_or(0);

    let mut mime_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .map_or("application/octet-stream".to_string(), |s| {
            s.trim().to_string()
        });

    if (mime_type == "application/octet-stream" || mime_type == "text/plain")
        && let (Some(name), Some(ext)) = filename_from_url(&resource.url)
    {
        let detection_name = format!("{}.{}", name, ext);
        if let Some(mime) = mime_guess::from_path(&detection_name).first_raw() {
            mime_type = mime.to_string();
        }
    }

    let (filename, mut extension) = match filename_from_url(&resource.url) {
        (Some(name), ext) => {
            let fname = resource.name.clone().unwrap_or(name);
            (fname, ext)
        }
        (None, ext) => (
            resource
                .name
                .clone()
                .unwrap_or_else(|| "downloaded_file".to_string()),
            ext,
        ),
    };

    if extension.is_none() {
        extension = mime_guess::get_mime_extensions_str(&mime_type)
            .and_then(|exts| exts.first().map(|&s| s.to_string()));
    }

    let extension = extension.unwrap_or_else(|| "bin".to_string());

    tracing::debug!(
        mime_type = %mime_type,
        "Using filename: {}, extension: {}",
        filename,
        extension,
    );

    Ok((
        response,
        DownloadInfo {
            filename,
            extension,
            content_length,
            mime_type,
        },
    ))
}
