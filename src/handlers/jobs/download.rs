use crate::{error::JobError, models::Resource};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use url::Url;
use wreq::Response;

use super::expand_path;
use crate::handlers::jobs::JobEnvelope;
use crate::handlers::jobs::types::DownloadInfo;

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

pub(crate) fn extract_file_job(job: &JobEnvelope) -> Result<&super::FileJob, JobError> {
    match job {
        JobEnvelope::File(file_job) => Ok(file_job),
        _ => Err(JobError::OtherFatal(
            "FileJobHandler received non-file job".into(),
        )),
    }
}

pub(crate) async fn download_to_temp(
    response: Response,
    temp_dir: &str,
    filename: &str,
    job_id: &str,
) -> Result<(String, String, String, u64), JobError> {
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

    Ok((temp_path, hash, bytes_mime, byte_count))
}

pub(crate) async fn initiate_download(
    resource: &Resource,
) -> Result<(Response, DownloadInfo), JobError> {
    let mut default_headers = wreq::header::HeaderMap::new();
    default_headers.insert(
        wreq::header::ACCEPT,
        wreq::header::HeaderValue::from_static(
            "video/webm,video/mp4,application/octet-stream,image/*,*/*;q=0.8",
        ),
    );

    let client = wreq::Client::builder()
        .emulation(wreq_util::Emulation::Chrome124)
        .default_headers(default_headers)
        .build()?;

    let mut request = client.get(resource.url.as_str());

    let origin = resource.url.origin();

    if let Ok(val) = wreq::header::HeaderValue::from_str(&origin.ascii_serialization()) {
        request = request.header(wreq::header::REFERER, val);
    }

    if let Some(headers) = &resource.config.as_ref().and_then(|c| c.headers.as_ref()) {
        if let Some(auth) = &headers.authorization {
            request = request.header(wreq::header::AUTHORIZATION, auth);
        }
        if let Some(cookie) = &headers.cookie {
            request = request.header(wreq::header::COOKIE, cookie);
        }
    }

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
        .get(wreq::header::CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());

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
