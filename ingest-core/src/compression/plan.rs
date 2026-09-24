//! Compression decision helper — replaces the 4-phase decision tree with a single call.
//!
//! `decide()` returns a `CompressionPlan` describing what to do.
//! `apply()` executes the plan and returns the result.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::compression::{
    CompressionOverride, GenericCompressionStrategy, ImageCompressionStrategy,
    VideoCompressionStrategy,
};
use crate::error::JobErrorOutcome;

/// A compression plan — describes what to do with a file.
#[derive(Debug, Clone)]
pub enum CompressionPlan {
    /// No compression — keep the original bytes.
    Passthrough,
    /// Image compression with a specific strategy.
    Image(ImageCompressionStrategy),
    /// Video compression with a specific strategy.
    Video(VideoCompressionStrategy),
    /// Generic compression with a specific strategy.
    Generic(GenericCompressionStrategy),
}

/// The result of applying a compression plan.
#[derive(Debug)]
pub struct AppliedCompression {
    /// Path to the compressed file (may be the same as input for passthrough).
    pub output_path: PathBuf,
    /// Size of the compressed file in bytes.
    pub size: u64,
    /// Final MIME type after compression.
    pub mime: String,
    /// The strategy that was actually applied (may differ from requested).
    pub applied_strategy: GenericCompressionStrategy,
}

/// Decide what compression to apply based on the override, MIME type, and config.
pub fn decide(
    override_strategy: Option<&CompressionOverride>,
    detected_mime: &str,
) -> CompressionPlan {
    match override_strategy {
        Some(CompressionOverride::Image(strategy)) => {
            if detected_mime.starts_with("image/") {
                CompressionPlan::Image(strategy.clone())
            } else {
                tracing::warn!(
                    "Image compression requested but MIME is {} — skipping",
                    detected_mime
                );
                CompressionPlan::Passthrough
            }
        }
        Some(CompressionOverride::Video(strategy)) => {
            if detected_mime.starts_with("video/") {
                CompressionPlan::Video(strategy.clone())
            } else {
                tracing::warn!(
                    "Video compression requested but MIME is {} — skipping",
                    detected_mime
                );
                CompressionPlan::Passthrough
            }
        }
        Some(CompressionOverride::Generic(strategy)) => CompressionPlan::Generic(strategy.clone()),
        Some(CompressionOverride::Universal(_)) | None => CompressionPlan::Passthrough,
    }
}

/// Pick a sensible default chunk compression strategy based on the source MIME type.
pub fn default_chunk_compression(mime: &str) -> GenericCompressionStrategy {
    match mime {
        // Archive formats
        "application/x-rar"
        | "application/vnd.rar"
        | "application/rar"
        | "application/zip"
        | "application/x-7z-compressed"
        | "application/x-tar"
        | "application/gzip"
        | "application/zstd" => GenericCompressionStrategy::OriginalFormat,
        // Already-compressed by nature — gzip would grow them
        "application/x-bzip2" | "application/x-xz" | "application/x-lz4" => {
            GenericCompressionStrategy::OriginalFormat
        }
        // Video
        "video/mp4" | "video/x-matroska" | "video/webm" | "video/quicktime" => {
            GenericCompressionStrategy::OriginalFormat
        }
        // Audio
        "audio/mpeg" | "audio/ogg" | "audio/flac" | "audio/mp4" | "audio/aac" => {
            GenericCompressionStrategy::OriginalFormat
        }
        // Image (most modern formats are already compressed)
        "image/jpeg" | "image/png" | "image/webp" | "image/avif" | "image/gif" => {
            GenericCompressionStrategy::OriginalFormat
        }
        // Documents
        "application/pdf" => GenericCompressionStrategy::OriginalFormat,
        // Compressible by default
        _ => GenericCompressionStrategy::Gzip,
    }
}

/// Apply a compression plan to a file.
///
/// Returns `Ok(AppliedCompression)` on success, or `Err(JobErrorOutcome)` on failure.
pub async fn apply(
    plan: &CompressionPlan,
    input_path: &Path,
    filename: &str,
    mime: &str,
    content_length: u64,
    quality: u8,
    timeout_secs: u64,
    cancel: Arc<AtomicBool>,
) -> Result<AppliedCompression, JobErrorOutcome> {
    let compression_timeout = std::time::Duration::from_secs(timeout_secs);

    match plan {
        CompressionPlan::Passthrough => {
            let size = std::fs::metadata(input_path)
                .map(|m| m.len())
                .unwrap_or(content_length);
            Ok(AppliedCompression {
                output_path: input_path.to_path_buf(),
                size,
                mime: mime.to_string(),
                applied_strategy: GenericCompressionStrategy::OriginalFormat,
            })
        }
        CompressionPlan::Image(strategy) => {
            let strategy = strategy.clone();
            let input_path = input_path.to_path_buf();
            let mime = mime.to_string();
            let filename = filename.to_string();

            match tokio::time::timeout(
                compression_timeout,
                crate::handlers::jobs::compression::compress_image_local(
                    &filename,
                    &mime,
                    content_length,
                    quality,
                    &input_path.to_string_lossy(),
                    &strategy,
                ),
            )
            .await
            {
                Ok(Ok((path, size, final_mime))) => {
                    tracing::info!("Image compressed: {} -> {} bytes", content_length, size);
                    Ok(AppliedCompression {
                        output_path: PathBuf::from(path),
                        size,
                        mime: final_mime,
                        applied_strategy: GenericCompressionStrategy::OriginalFormat,
                    })
                }
                Ok(Err(e)) => {
                    tracing::warn!("Image compression failed: {e}, keeping original");
                    Ok(AppliedCompression {
                        output_path: input_path.to_path_buf(),
                        size: content_length,
                        mime: mime.to_string(),
                        applied_strategy: GenericCompressionStrategy::OriginalFormat,
                    })
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        "Image compression timed out after {}s, keeping original",
                        timeout_secs
                    );
                    Ok(AppliedCompression {
                        output_path: input_path.to_path_buf(),
                        size: content_length,
                        mime: mime.to_string(),
                        applied_strategy: GenericCompressionStrategy::OriginalFormat,
                    })
                }
            }
        }
        CompressionPlan::Video(strategy) => {
            let strategy = strategy.clone();
            let input_path = input_path.to_path_buf();
            let mime = mime.to_string();
            let filename = filename.to_string();

            match tokio::time::timeout(
                compression_timeout,
                crate::handlers::jobs::compression::compress_video_local(
                    &filename,
                    &mime,
                    content_length,
                    quality,
                    &input_path.to_string_lossy(),
                    &strategy,
                    cancel.clone(),
                ),
            )
            .await
            {
                Ok(Ok((path, size, final_mime))) => {
                    tracing::info!("Video compressed: {} -> {} bytes", content_length, size);
                    Ok(AppliedCompression {
                        output_path: PathBuf::from(path),
                        size,
                        mime: final_mime,
                        applied_strategy: GenericCompressionStrategy::OriginalFormat,
                    })
                }
                Ok(Err(e)) => {
                    tracing::warn!("Video compression failed: {e}, keeping original");
                    Ok(AppliedCompression {
                        output_path: input_path.to_path_buf(),
                        size: content_length,
                        mime: mime.to_string(),
                        applied_strategy: GenericCompressionStrategy::OriginalFormat,
                    })
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        "Video compression timed out after {}s, keeping original",
                        timeout_secs
                    );
                    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                    Ok(AppliedCompression {
                        output_path: input_path.to_path_buf(),
                        size: content_length,
                        mime: mime.to_string(),
                        applied_strategy: GenericCompressionStrategy::OriginalFormat,
                    })
                }
            }
        }
        CompressionPlan::Generic(strategy) => {
            let strategy = strategy.clone();
            let input_path = input_path.to_path_buf();
            let filename = filename.to_string();

            match tokio::time::timeout(
                compression_timeout,
                crate::handlers::jobs::compression::compress_generic_local(
                    &input_path.to_string_lossy(),
                    &filename,
                    &strategy,
                    quality,
                    cancel.clone(),
                ),
            )
            .await
            {
                Ok(Ok((path, size, applied))) => {
                    let final_mime = if path == input_path.to_string_lossy() {
                        mime.to_string()
                    } else {
                        crate::handlers::jobs::compression::generic_compression_mime(&applied)
                            .to_string()
                    };
                    tracing::info!("Generic compressed: {} -> {} bytes", content_length, size);
                    Ok(AppliedCompression {
                        output_path: PathBuf::from(path),
                        size,
                        mime: final_mime,
                        applied_strategy: applied,
                    })
                }
                Ok(Err(e)) => {
                    tracing::warn!("Generic compression failed: {e}, keeping original");
                    Ok(AppliedCompression {
                        output_path: input_path.to_path_buf(),
                        size: content_length,
                        mime: mime.to_string(),
                        applied_strategy: GenericCompressionStrategy::OriginalFormat,
                    })
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        "Generic compression timed out after {}s, keeping original",
                        timeout_secs
                    );
                    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                    Ok(AppliedCompression {
                        output_path: input_path.to_path_buf(),
                        size: content_length,
                        mime: mime.to_string(),
                        applied_strategy: GenericCompressionStrategy::OriginalFormat,
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decide_passthrough_for_universal() {
        let plan = decide(None, "application/octet-stream");
        assert!(matches!(plan, CompressionPlan::Passthrough));
    }

    #[test]
    fn test_decide_image_for_image_mime() {
        let override_strategy = Some(CompressionOverride::Image(ImageCompressionStrategy::Avif));
        let plan = decide(override_strategy.as_ref(), "image/png");
        assert!(matches!(plan, CompressionPlan::Image(_)));
    }

    #[test]
    fn test_decide_passthrough_for_image_on_video_mime() {
        let override_strategy = Some(CompressionOverride::Image(ImageCompressionStrategy::Avif));
        let plan = decide(override_strategy.as_ref(), "video/mp4");
        assert!(matches!(plan, CompressionPlan::Passthrough));
    }

    #[test]
    fn test_decide_video_for_video_mime() {
        let override_strategy = Some(CompressionOverride::Video(VideoCompressionStrategy::H264));
        let plan = decide(override_strategy.as_ref(), "video/mp4");
        assert!(matches!(plan, CompressionPlan::Video(_)));
    }

    #[test]
    fn test_decide_passthrough_for_video_on_image_mime() {
        let override_strategy = Some(CompressionOverride::Video(VideoCompressionStrategy::H264));
        let plan = decide(override_strategy.as_ref(), "image/png");
        assert!(matches!(plan, CompressionPlan::Passthrough));
    }

    #[test]
    fn test_decide_generic() {
        let override_strategy = Some(CompressionOverride::Generic(
            GenericCompressionStrategy::Gzip,
        ));
        let plan = decide(override_strategy.as_ref(), "text/plain");
        assert!(matches!(
            plan,
            CompressionPlan::Generic(GenericCompressionStrategy::Gzip)
        ));
    }

    #[test]
    fn test_default_chunk_compression_passthrough_for_compressed_formats() {
        let formats = [
            "application/x-rar",
            "application/zip",
            "application/x-7z-compressed",
            "video/mp4",
            "video/webm",
            "audio/mpeg",
            "image/jpeg",
            "image/png",
            "application/pdf",
        ];
        for mime in formats {
            let strategy = default_chunk_compression(mime);
            assert_eq!(
                strategy,
                GenericCompressionStrategy::OriginalFormat,
                "Expected OriginalFormat for {mime}"
            );
        }
    }

    #[test]
    fn test_default_chunk_compression_gzip_for_text() {
        let formats = ["text/plain", "text/html", "application/json", "text/csv"];
        for mime in formats {
            let strategy = default_chunk_compression(mime);
            assert_eq!(
                strategy,
                GenericCompressionStrategy::Gzip,
                "Expected Gzip for {mime}"
            );
        }
    }
}
