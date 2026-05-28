use std::{
    path::Path,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::{
    error::JobError,
    models::{ImageCompressionStrategy, VideoCompressionStrategy},
};

pub(crate) fn mime_to_extension(mime: &str) -> Option<&'static str> {
    match mime {
        "image/webp" => Some("webp"),
        "image/avif" => Some("avif"),
        "video/mp4" => Some("mp4"),
        _ => None,
    }
}

pub(crate) async fn compress_image_local(
    original_name: &str,
    mime_type: &str,
    original_size: u64,
    _quality: u8,
    temp_path: &str,
    strategy: &ImageCompressionStrategy,
) -> Result<(String, u64, String), JobError> {
    use image::{
        ExtendedColorType, ImageEncoder, ImageFormat, ImageReader,
        codecs::{avif::AvifEncoder, webp::WebPEncoder},
    };
    use std::fs::File;
    use std::io::BufWriter;

    if mime_type == "image/webp" {
        let meta = std::fs::metadata(temp_path)?;
        return Ok((temp_path.to_string(), meta.len(), mime_type.to_string()));
    }

    let format = match mime_type {
        "image/jpeg" | "image/jpg" => ImageFormat::Jpeg,
        "image/png" => ImageFormat::Png,
        "image/gif" => ImageFormat::Gif,
        _ => {
            return Err(JobError::OtherFatal(
                "Unsupported image format for compression".into(),
            ));
        }
    };

    tracing::info!("Compressing image: {}", original_name);
    let mut reader = ImageReader::open(temp_path)?;
    reader.set_format(format);
    let img = reader.decode()?;
    let rgba = img.to_rgba8();
    let (width, height) = rgba.dimensions();

    let ext = match strategy {
        ImageCompressionStrategy::Avif => "avif",
        _ => "webp",
    };

    let parent = Path::new(temp_path).parent().unwrap_or(Path::new("."));
    let output_path = parent.join(original_name).with_extension(ext);
    let output_path_str = output_path.to_string_lossy().to_string();

    tracing::debug!(
        "Decoding image for compression with strategy: {:?}, on temp_path: {}",
        strategy,
        temp_path
    );
    match strategy {
        ImageCompressionStrategy::Avif => {
            let file = File::create(&output_path)?;
            AvifEncoder::new(BufWriter::new(file)).write_image(
                rgba.as_raw(),
                width,
                height,
                ExtendedColorType::Rgba8,
            )?;
        }
        ImageCompressionStrategy::LosslessWebp | ImageCompressionStrategy::Webp => {
            let file = File::create(&output_path)?;
            WebPEncoder::new_lossless(BufWriter::new(file)).write_image(
                rgba.as_raw(),
                width,
                height,
                ExtendedColorType::Rgba8,
            )?;
        }
    }

    let compressed_size = std::fs::metadata(&output_path)?.len();

    if compressed_size >= original_size {
        std::fs::remove_file(&output_path)?;
        let meta = std::fs::metadata(temp_path)?;
        return Ok((temp_path.to_string(), meta.len(), mime_type.to_string()));
    }

    std::fs::remove_file(temp_path)?;
    let final_mime = match strategy {
        ImageCompressionStrategy::Avif => "image/avif",
        _ => "image/webp",
    };
    Ok((output_path_str, compressed_size, final_mime.to_string()))
}

pub(crate) async fn compress_video_local(
    original_name: &str,
    original_mime: &str,
    original_size: u64,
    quality: u8,
    temp_path: &str,
    strategy: &VideoCompressionStrategy,
    cancelled: Arc<AtomicBool>,
) -> Result<(String, u64, String), JobError> {
    use ffmpeg_next::{Dictionary, Packet, codec, encoder, format, frame, media, picture};

    let temp_path = temp_path.to_string();
    let original_name = original_name.to_string();
    let original_mime = original_mime.to_string();
    let strategy = strategy.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<(String, u64, String), JobError> {
        let mut ictx = format::input(&temp_path)
            .map_err(|e| JobError::OtherFatal(format!("Failed to open input video: {e}")))?;

        let input = ictx
            .streams()
            .best(media::Type::Video)
            .ok_or_else(|| JobError::OtherFatal("No video stream found in input".into()))?;
        let video_stream_index = input.index();

        let decoder_ctx = codec::context::Context::from_parameters(input.parameters())
            .map_err(|e| JobError::OtherFatal(format!("Failed to create decoder context: {e}")))?;
        let mut decoder = decoder_ctx
            .decoder()
            .video()
            .map_err(|e| JobError::OtherFatal(format!("Failed to open video decoder: {e}")))?;

        let (encoder_id, _codec_name, crf_val) = match strategy {
            VideoCompressionStrategy::H264 => {
                let crf = ((100 - quality as u32) * 51 / 100) as i32;
                (codec::Id::H264, "libx264", crf)
            }
            VideoCompressionStrategy::H265 => {
                let crf = ((100 - quality as u32) * 51 / 100) as i32;
                (codec::Id::H265, "libx265", crf)
            }
            VideoCompressionStrategy::Av1 => {
                let crf = (((100 - quality as u32) * 63 / 100).max(15)) as i32;
                (codec::Id::AV1, "libaom-av1", crf)
            }
        };

        let parent = Path::new(&temp_path).parent().unwrap_or(Path::new("."));
        let output_path = parent.join(&original_name).with_extension("mp4");
        let output_path_str = output_path.to_string_lossy().to_string();

        tracing::info!(
            "Compressing video: {}, strategy: {:?}",
            output_path_str,
            strategy
        );
        let mut octx = format::output(&output_path_str)
            .map_err(|e| JobError::OtherFatal(format!("Failed to create output context: {e}")))?;

        let global_header = octx.format().flags().contains(format::Flags::GLOBAL_HEADER);

        let encoder_codec = encoder::find(encoder_id)
            .ok_or_else(|| JobError::OtherFatal(format!("Encoder not found on this system")))?;

        let mut ost = octx
            .add_stream(encoder_codec)
            .map_err(|e| JobError::OtherFatal(format!("Failed to add output stream: {e}")))?;

        let mut encoder = codec::context::Context::new_with_codec(encoder_codec)
            .encoder()
            .video()
            .map_err(|e| JobError::OtherFatal(format!("Failed to get video encoder: {e}")))?;

        ost.set_parameters(&encoder);

        encoder.set_height(decoder.height());
        encoder.set_width(decoder.width());
        encoder.set_aspect_ratio(decoder.aspect_ratio());
        encoder.set_format(decoder.format());
        encoder.set_frame_rate(decoder.frame_rate());
        encoder.set_time_base(input.time_base());

        if global_header {
            encoder.set_flags(codec::Flags::GLOBAL_HEADER);
        }

        let mut opts = Dictionary::new();
        opts.set("crf", &crf_val.to_string());
        match strategy {
            VideoCompressionStrategy::Av1 => {
                opts.set("cpu-used", "4");
            }
            _ => {
                opts.set("preset", "medium");
            }
        }

        let mut encoder = encoder
            .open_with(opts)
            .map_err(|e| JobError::OtherFatal(format!("Failed to open encoder: {e}")))?;
        ost.set_parameters(&encoder);

        octx.set_metadata(ictx.metadata().to_owned());
        octx.write_header()
            .map_err(|e| JobError::OtherFatal(format!("Failed to write header: {e}")))?;

        let ist_time_base = input.time_base();
        let ost_time_base = octx.stream(0).unwrap().time_base();

        let mut packet_count = 0u64;
        let log_interval = 500u64;
        for (stream, packet) in ictx.packets() {
            if cancelled.load(Ordering::Relaxed) {
                tracing::warn!("Video compression cancelled due to timeout");
                let _ = std::fs::remove_file(&output_path_str);
                return Err(JobError::OtherFatal(
                    "Video compression cancelled due to timeout".into(),
                ));
            }

            if stream.index() != video_stream_index {
                continue;
            }

            packet_count += 1;
            if packet_count % log_interval == 0 {
                tracing::info!(
                    "Video compression progress: {} packets processed",
                    packet_count
                );
            }

            decoder
                .send_packet(&packet)
                .map_err(|e| JobError::OtherFatal(format!("Decoder send_packet: {e}")))?;

            let mut frame = frame::Video::empty();
            while decoder.receive_frame(&mut frame).is_ok() {
                let pts = frame.timestamp();
                frame.set_pts(pts);
                frame.set_kind(picture::Type::None);

                encoder
                    .send_frame(&frame)
                    .map_err(|e| JobError::OtherFatal(format!("Encoder send_frame: {e}")))?;

                let mut encoded = Packet::empty();
                while encoder.receive_packet(&mut encoded).is_ok() {
                    encoded.set_stream(0);
                    encoded.rescale_ts(ist_time_base, ost_time_base);
                    encoded
                        .write_interleaved(&mut octx)
                        .map_err(|e| JobError::OtherFatal(format!("Write packet: {e}")))?;
                }
            }
        }

        tracing::info!("Video compression finished: {} total packets", packet_count);

        decoder
            .send_eof()
            .map_err(|e| JobError::OtherFatal(format!("Decoder send_eof: {e}")))?;
        let mut frame = frame::Video::empty();
        while decoder.receive_frame(&mut frame).is_ok() {
            let pts = frame.timestamp();
            frame.set_pts(pts);
            frame.set_kind(picture::Type::None);

            encoder
                .send_frame(&frame)
                .map_err(|e| JobError::OtherFatal(format!("Encoder send_frame: {e}")))?;

            let mut encoded = Packet::empty();
            while encoder.receive_packet(&mut encoded).is_ok() {
                encoded.set_stream(0);
                encoded.rescale_ts(ist_time_base, ost_time_base);
                encoded
                    .write_interleaved(&mut octx)
                    .map_err(|e| JobError::OtherFatal(format!("Write packet: {e}")))?;
            }
        }

        encoder
            .send_eof()
            .map_err(|e| JobError::OtherFatal(format!("Encoder send_eof: {e}")))?;
        let mut encoded = Packet::empty();
        while encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(0);
            encoded.rescale_ts(ist_time_base, ost_time_base);
            encoded
                .write_interleaved(&mut octx)
                .map_err(|e| JobError::OtherFatal(format!("Write packet: {e}")))?;
        }

        octx.write_trailer()
            .map_err(|e| JobError::OtherFatal(format!("Failed to write trailer: {e}")))?;

        let compressed_size = std::fs::metadata(&output_path_str)
            .map_err(|e| JobError::IoError(e))?
            .len();

        if original_size > 0 && compressed_size >= original_size {
            let _ = std::fs::remove_file(&output_path_str);
            let meta = std::fs::metadata(&temp_path)
                .map_err(|e| JobError::IoError(e))?
                .len();
            return Ok((temp_path, meta, original_mime));
        }

        let _ = std::fs::remove_file(&temp_path);
        Ok((output_path_str, compressed_size, "video/mp4".to_string()))
    })
    .await
    .map_err(|e| JobError::OtherFatal(format!("Spawn blocking failed: {e}")))??;

    Ok(result)
}
