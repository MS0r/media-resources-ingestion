use ffmpeg_next::{
    ChannelLayout, Dictionary, Packet, Rational, codec,
    decoder::{self, Video},
    encoder, filter,
    format::{
        self,
        context::{Input, Output},
    },
    frame, media, picture,
};
use tokio::sync::mpsc::{Receiver, Sender};

use std::{
    io,
    path::Path,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::{
    error::JobError,
    models::{GenericCompressionStrategy, ImageCompressionStrategy, VideoCompressionStrategy},
};

enum MuxItem {
    Video(frame::Video),
    Audio(frame::Audio),
}

impl From<tokio::sync::mpsc::error::SendError<MuxItem>> for JobError {
    fn from(_: tokio::sync::mpsc::error::SendError<MuxItem>) -> Self {
        JobError::OtherFatal("Compression channel closed unexpectedly".into())
    }
}

pub(crate) fn mime_to_extension(mime: &str) -> Option<&'static str> {
    match mime {
        "image/webp" => Some("webp"),
        "image/avif" => Some("avif"),
        "video/mp4" => Some("mp4"),
        "application/gzip" => Some("gz"),
        "application/zstd" => Some("zst"),
        "application/zip" => Some("zip"),
        "application/x-7z-compressed" => Some("7z"),
        "application/x-rar" => Some("rar"),
        _ => None,
    }
}

pub(crate) fn generic_compression_extension(strategy: &GenericCompressionStrategy) -> &'static str {
    match strategy {
        GenericCompressionStrategy::Gzip => "gz",
        GenericCompressionStrategy::Zstd => "zst",
        GenericCompressionStrategy::Zip => "zip",
        GenericCompressionStrategy::SevenZ => "7z",
        GenericCompressionStrategy::OriginalFormat | GenericCompressionStrategy::None => "",
    }
}

pub(crate) fn generic_compression_mime(strategy: &GenericCompressionStrategy) -> &'static str {
    match strategy {
        GenericCompressionStrategy::Gzip => "application/gzip",
        GenericCompressionStrategy::Zstd => "application/zstd",
        GenericCompressionStrategy::Zip => "application/zip",
        GenericCompressionStrategy::SevenZ => "application/x-7z-compressed",
        GenericCompressionStrategy::OriginalFormat | GenericCompressionStrategy::None => "",
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

fn setup_audio_filter(
    decoder: &decoder::Audio,
    encoder: &codec::encoder::audio::Encoder,
) -> Result<filter::Graph, JobError> {
    let mut graph = filter::Graph::new();

    let args = format!(
        "time_base={}:sample_rate={}:sample_fmt={}:channel_layout=0x{:x}",
        decoder.time_base(),
        decoder.rate(),
        decoder.format().name(),
        decoder.channel_layout().bits()
    );
    graph.add(
        &filter::find("abuffer").ok_or_else(|| {
            JobError::OtherFatal("abuffer filter not found on this system".into())
        })?,
        "in",
        &args,
    )?;
    graph.add(
        &filter::find("abuffersink").ok_or_else(|| {
            JobError::OtherFatal("abuffersink filter not found on this system".into())
        })?,
        "out",
        "",
    )?;

    {
        let mut out = graph
            .get("out")
            .ok_or_else(|| JobError::OtherFatal("Failed to get 'out' filter context".into()))?;
        out.set_sample_format(encoder.format());
        out.set_channel_layout(encoder.channel_layout());
        out.set_sample_rate(encoder.rate());
    }

    graph.output("in", 0)?.input("out", 0)?.parse("anull")?;
    graph.validate()?;

    if let Some(codec) = encoder.codec() {
        if !codec
            .capabilities()
            .contains(ffmpeg_next::codec::capabilities::Capabilities::VARIABLE_FRAME_SIZE)
        {
            graph
                .get("out")
                .ok_or_else(|| {
                    JobError::OtherFatal("Failed to get 'out' filter context for frame size".into())
                })?
                .sink()
                .set_frame_size(encoder.frame_size());
        }
    }

    Ok(graph)
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
    let temp_path = temp_path.to_string();
    let original_name = original_name.to_string();
    let original_mime = original_mime.to_string();
    let strategy = strategy.clone();
    let cancelled_decode = cancelled.clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<MuxItem>(4);

    let mut ictx = format::input(&temp_path)?;

    let (video_stream_index, video_istb, mut video_decoder) = {
        let input = ictx
            .streams()
            .best(media::Type::Video)
            .ok_or_else(|| JobError::OtherFatal("No video stream found in input".into()))?;
        let index = input.index();
        let time_base = input.time_base();
        let decoder_ctx = codec::context::Context::from_parameters(input.parameters())?;
        let decoder = decoder_ctx.decoder().video()?;

        (index, time_base, decoder)
    };

    let (audio_stream_index, audio_istb, mut audio_decoder) = {
        let input_audio = ictx.streams().best(media::Type::Audio);
        let index = input_audio.as_ref().map(|s| s.index());
        let istb = input_audio.as_ref().map(|s| s.time_base());
        let decoder = input_audio
            .map(|s| -> Result<decoder::Audio, JobError> {
                let ctx = codec::context::Context::from_parameters(s.parameters())?;
                Ok(ctx.decoder().audio()?)
            })
            .transpose()?;

        (index, istb, decoder)
    };

    // --- Video encoder setup ---
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

    let output_path_str = {
        let parent = Path::new(&temp_path).parent().unwrap_or(Path::new("."));
        let output_path = parent.join(&original_name).with_extension("mp4");
        let outstr = output_path.to_string_lossy().to_string();

        outstr
    };

    tracing::info!(
        "Compressing video: {}, strategy: {:?}",
        output_path_str,
        strategy
    );
    let mut octx = format::output(&output_path_str)?;

    let global_header = octx.format().flags().contains(format::Flags::GLOBAL_HEADER);

    let encoder_codec = encoder::find(encoder_id)
        .ok_or_else(|| JobError::OtherFatal("Encoder not found on this system".to_string()))?;

    let supported_format = encoder_codec.video().map(|v| -> format::Pixel {
        if let Some(mut formats) = v.formats()
            && let Some(f) = formats.next()
        {
            return f;
        }
        format::Pixel::YUV420P
    })?;

    let mut video_encoder = codec::context::Context::new_with_codec(encoder_codec)
        .encoder()
        .video()?;

    video_encoder.set_height(video_decoder.height());
    video_encoder.set_width(video_decoder.width());
    video_encoder.set_aspect_ratio(video_decoder.aspect_ratio());
    video_encoder.set_format(supported_format);
    video_encoder.set_frame_rate(video_decoder.frame_rate());
    video_encoder.set_time_base(video_istb);

    if global_header {
        video_encoder.set_flags(codec::Flags::GLOBAL_HEADER);
    }

    let mut video_encoder = {
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
        video_encoder.open_with(opts)?
    };

    {
        let mut ost = octx.add_stream(encoder_codec)?;
        ost.set_parameters(&video_encoder);
    }
    

    let mut audio_encoder = None;
    let mut audio_filter = None;
    let mut audio_ost_index = None;
    let mut audio_ostb = None;

    if let Some(ref mut audio_dec) = audio_decoder {
        let aac_codec = encoder::find(codec::Id::AAC).ok_or_else(|| {
            JobError::OtherFatal("AAC encoder not found on this system".to_string())
        })?;
        let aac_codec_info = aac_codec.audio()?;

        let mut enc = codec::context::Context::new_with_codec(aac_codec)
            .encoder()
            .audio()?;

        let channel_layout = aac_codec_info
            .channel_layouts()
            .map(|cls| cls.best(audio_dec.channel_layout().channels()))
            .unwrap_or(ChannelLayout::STEREO);

        if global_header {
            enc.set_flags(codec::Flags::GLOBAL_HEADER);
        }
        enc.set_rate(audio_dec.rate() as i32);
        enc.set_channel_layout(channel_layout);
        enc.set_format(
            aac_codec_info
                .formats()
                .ok_or_else(|| JobError::OtherFatal("AAC encoder has no supported formats".into()))?
                .next()
                .ok_or_else(|| {
                    JobError::OtherFatal("AAC encoder has no supported formats".into())
                })?,
        );
        enc.set_bit_rate((64000 + (quality as u32) * 1280) as usize);
        enc.set_time_base((1, audio_dec.rate() as i32));

        let enc = enc.open_as(aac_codec_info)?;

        {
            let mut ost = octx.add_stream(aac_codec)?;
            ost.set_parameters(&enc);
            ost.set_time_base((1, audio_dec.rate() as i32));
        }

        let ost_idx = octx.streams().count() - 1;

        let filter = setup_audio_filter(audio_dec, &enc)?;

        audio_encoder = Some(enc);
        audio_filter = Some(filter);
        audio_ost_index = Some(ost_idx);
    }

    octx.set_metadata(ictx.metadata().to_owned());    
    octx.write_header()?;

    if let Some(ost_idx) = audio_ost_index {
        audio_ostb = Some(octx
            .stream(ost_idx)
            .ok_or_else(|| JobError::OtherFatal("Missing audio output stream".into()))?
            .time_base()
        );
    }

    let video_ostb = octx
        .stream(0)
        .ok_or_else(|| JobError::OtherFatal("No output stream 0".to_string()))?
        .time_base();

    let out_path = output_path_str.clone();
    let decode_task = tokio::task::spawn_blocking(move || -> Result<u64, JobError> {
        decode_av_frames(
            &mut ictx,
            &mut video_decoder,
            audio_decoder.as_mut(),
            video_stream_index,
            audio_stream_index,
            &out_path,
            tx,
            cancelled_decode,
        )
    });

    let encode_task = tokio::task::spawn_blocking(move || -> Result<(), JobError> {
        encode_av_packets(
            &mut octx,
            &mut video_encoder,
            audio_encoder.as_mut(),
            audio_filter.as_mut(),
            &mut rx,
            video_istb,
            video_ostb,
            audio_istb.unwrap_or(Rational(0, 1)),
            audio_ostb.unwrap_or(Rational(0, 1)),
            audio_ost_index,
        )?;
        octx.write_trailer().map_err(|e| JobError::from(e))
    });

    let (packet_count, _) = tokio::try_join!(decode_task, encode_task)?;

    tracing::info!(
        "Video compression finished: {} total packets",
        packet_count?
    );

    let compressed_size = std::fs::metadata(&output_path_str)?.len();

    if original_size > 0 && compressed_size >= original_size {
        let _ = std::fs::remove_file(&output_path_str);
        let meta = std::fs::metadata(&temp_path)?.len();
        return Ok((temp_path, meta, original_mime));
    }

    let _ = std::fs::remove_file(&temp_path);
    Ok((output_path_str, compressed_size, "video/mp4".to_string()))
}

fn decode_av_frames(
    ictx: &mut Input,
    video_decoder: &mut Video,
    mut audio_decoder: Option<&mut decoder::Audio>,
    video_stream_index: usize,
    audio_stream_index: Option<usize>,
    out_path: &str,
    tx: Sender<MuxItem>,
    cancelled: Arc<AtomicBool>,
) -> Result<u64, JobError> {
    let mut packet_count = 0u64;
    let log_interval = 500u64;

    for (stream, packet) in ictx.packets() {
        if cancelled.load(Ordering::Relaxed) {
            tracing::warn!("Video compression cancelled due to timeout");
            let _ = std::fs::remove_file(out_path);
            return Err(JobError::OtherFatal(
                "Video compression cancelled due to timeout".into(),
            ));
        }

        let stream_idx = stream.index();

        if stream_idx == video_stream_index {
            packet_count += 1;
            if packet_count.is_multiple_of(log_interval) {
                tracing::info!(
                    "Video compression progress: {} packets processed",
                    packet_count
                );
            }

            video_decoder.send_packet(&packet)?;
            let mut frame = frame::Video::empty();
            while video_decoder.receive_frame(&mut frame).is_ok() {
                let pts = frame.timestamp();
                frame.set_pts(pts);
                frame.set_kind(picture::Type::None);
                tx.blocking_send(MuxItem::Video(frame))?;
                frame = frame::Video::empty();
            }
        } else if let Some(audio_idx) = audio_stream_index
            && let Some(ref mut audio_dec) = audio_decoder
            && stream_idx == audio_idx
        {
            audio_dec.send_packet(&packet)?;
            let mut frame = frame::Audio::empty();
            while audio_dec.receive_frame(&mut frame).is_ok() {
                let pts = frame.timestamp();
                frame.set_pts(pts);
                tx.blocking_send(MuxItem::Audio(frame))?;
                frame = frame::Audio::empty();
            }
        }
    }

    // Flush video decoder
    video_decoder.send_eof()?;
    let mut frame = frame::Video::empty();
    while video_decoder.receive_frame(&mut frame).is_ok() {
        let pts = frame.timestamp();
        frame.set_pts(pts);
        frame.set_kind(picture::Type::None);
        tx.blocking_send(MuxItem::Video(frame))?;
        frame = frame::Video::empty();
    }

    // Flush audio decoder
    if let Some(ref mut audio_dec) = audio_decoder {
        audio_dec.send_eof()?;
        let mut frame = frame::Audio::empty();
        while audio_dec.receive_frame(&mut frame).is_ok() {
            let pts = frame.timestamp();
            frame.set_pts(pts);
            tx.blocking_send(MuxItem::Audio(frame))?;
            frame = frame::Audio::empty();
        }
    }

    Ok(packet_count)
}

fn encode_av_packets(
    octx: &mut Output,
    video_encoder: &mut encoder::Video,
    mut audio_encoder: Option<&mut codec::encoder::audio::Encoder>,
    mut audio_filter: Option<&mut filter::Graph>,
    rx: &mut Receiver<MuxItem>,
    video_istb: Rational,
    video_ostb: Rational,
    audio_istb: Rational,
    audio_ostb: Rational,
    audio_ost_index: Option<usize>,
) -> Result<(), JobError> {
    while let Some(item) = rx.blocking_recv() {
        match item {
            MuxItem::Video(frame) => {
                video_encoder.send_frame(&frame)?;

                let mut encoded = Packet::empty();
                while video_encoder.receive_packet(&mut encoded).is_ok() {
                    encoded.set_stream(0);
                    encoded.rescale_ts(video_istb, video_ostb);
                    encoded.write_interleaved(octx)?;
                }
            }
            MuxItem::Audio(frame) => {
                if let Some(ref mut afilt) = audio_filter
                    && let Some(ref mut aenc) = audio_encoder
                {
                    let ast_idx = audio_ost_index.unwrap_or(1);

                    afilt
                        .get("in")
                        .ok_or_else(|| JobError::OtherFatal("Missing 'in' filter context".into()))?
                        .source()
                        .add(&frame)?;

                    let mut filtered = frame::Audio::empty();
                    while afilt
                        .get("out")
                        .ok_or_else(|| JobError::OtherFatal("Missing 'out' filter context".into()))?
                        .sink()
                        .frame(&mut filtered)
                        .is_ok()
                    {
                        aenc.send_frame(&filtered)?;

                        let mut encoded = Packet::empty();
                        while aenc.receive_packet(&mut encoded).is_ok() {
                            encoded.set_stream(ast_idx);
                            encoded.rescale_ts(audio_istb, audio_ostb);
                            encoded.write_interleaved(octx)?;
                        }
                        filtered = frame::Audio::empty();
                    }
                }
            }
        }
    }

    // Flush audio filter
    if let Some(ref mut afilt) = audio_filter {
        if let Some(ref mut aenc) = audio_encoder {
            let ast_idx = audio_ost_index.unwrap_or(1);

            afilt
                .get("in")
                .ok_or_else(|| {
                    JobError::OtherFatal("Missing 'in' filter context during flush".into())
                })?
                .source()
                .flush()?;

            let mut filtered = frame::Audio::empty();
            while afilt
                .get("out")
                .ok_or_else(|| {
                    JobError::OtherFatal("Missing 'out' filter context during flush".into())
                })?
                .sink()
                .frame(&mut filtered)
                .is_ok()
            {
                aenc.send_frame(&filtered)?;

                let mut encoded = Packet::empty();
                while aenc.receive_packet(&mut encoded).is_ok() {
                    encoded.set_stream(ast_idx);
                    encoded.rescale_ts(audio_istb, audio_ostb);
                    encoded.write_interleaved(octx)?;
                }
                filtered = frame::Audio::empty();
            }
        }
    }

    // Flush video encoder
    video_encoder.send_eof()?;
    {
        let mut encoded = Packet::empty();
        while video_encoder.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(0);
            encoded.rescale_ts(video_istb, video_ostb);
            encoded.write_interleaved(octx)?;
        }
    }

    // Flush audio encoder
    if let Some(ref mut aenc) = audio_encoder {
        aenc.send_eof()?;
        let ast_idx = audio_ost_index.unwrap_or(1);
        let mut encoded = Packet::empty();
        while aenc.receive_packet(&mut encoded).is_ok() {
            encoded.set_stream(ast_idx);
            encoded.rescale_ts(audio_istb, audio_ostb);
            encoded.write_interleaved(octx)?;
        }
    }

    Ok(())
}

pub(crate) async fn compress_generic_local(
    temp_path: &str,
    original_name: &str,
    strategy: &GenericCompressionStrategy,
    quality: u8,
) -> Result<(String, u64), JobError> {
    match strategy {
        GenericCompressionStrategy::OriginalFormat | GenericCompressionStrategy::None => {
            let meta = std::fs::metadata(temp_path)?;
            return Ok((temp_path.to_string(), meta.len()));
        }
        GenericCompressionStrategy::Gzip
        | GenericCompressionStrategy::Zstd
        | GenericCompressionStrategy::Zip
        | GenericCompressionStrategy::SevenZ => {}
    }

    let parent = Path::new(temp_path).parent().unwrap_or(Path::new("."));
    let ext = generic_compression_extension(strategy);
    let output_path = parent.join(original_name).with_extension(ext);
    let output_path_str = output_path.to_string_lossy().to_string();

    let input_path = temp_path.to_string();
    let out_path = output_path_str.clone();
    let strategy = strategy.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<(String, u64), JobError> {
        match strategy {
            GenericCompressionStrategy::Gzip => {
                let mut input = std::fs::File::open(&input_path)?;
                let output = std::fs::File::create(&out_path)?;
                let mut encoder =
                    flate2::write::GzEncoder::new(output, flate2::Compression::default());
                io::copy(&mut input, &mut encoder)?;
                encoder.finish()?;
            }
            GenericCompressionStrategy::Zstd => {
                let mut input = std::fs::File::open(&input_path)?;
                let output = std::fs::File::create(&out_path)?;
                let level = (quality as i32).clamp(1, 22);
                let mut encoder = zstd::stream::Encoder::new(output, level)?;
                io::copy(&mut input, &mut encoder)?;
                encoder.finish()?;
            }
            GenericCompressionStrategy::Zip => {
                let output = std::fs::File::create(&out_path)?;
                let mut zip = zip::ZipWriter::new(output);
                let fname = Path::new(&input_path)
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "file".to_string());
                zip.start_file(fname, zip::write::SimpleFileOptions::default())?;
                let mut input = std::fs::File::open(&input_path)?;
                io::copy(&mut input, &mut zip)?;
                zip.finish()?;
            }
            GenericCompressionStrategy::SevenZ => {
                let mut writer = sevenz_rust::SevenZWriter::create(&out_path)?;
                writer.push_source_path(Path::new(&input_path), |_| true)?;
                writer.finish()?;
            }
            GenericCompressionStrategy::OriginalFormat | GenericCompressionStrategy::None => {}
        }

        let compressed_size = std::fs::metadata(&out_path)?.len();
        Ok((out_path, compressed_size))
    })
    .await??;

    // Keep compressed only if it's actually smaller
    let original_size = std::fs::metadata(temp_path)?.len();
    if original_size > 0 && result.1 >= original_size {
        std::fs::remove_file(&result.0).ok();
        return Ok((temp_path.to_string(), original_size));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::GenericCompressionStrategy;

    // ── Helper function tests ──────────────────────────────────────────────────

    #[test]
    fn test_generic_compression_extension() {
        assert_eq!(
            generic_compression_extension(&GenericCompressionStrategy::Gzip),
            "gz"
        );
        assert_eq!(
            generic_compression_extension(&GenericCompressionStrategy::Zstd),
            "zst"
        );
        assert_eq!(
            generic_compression_extension(&GenericCompressionStrategy::Zip),
            "zip"
        );
        assert_eq!(
            generic_compression_extension(&GenericCompressionStrategy::SevenZ),
            "7z"
        );
        assert_eq!(
            generic_compression_extension(&GenericCompressionStrategy::OriginalFormat),
            ""
        );
        assert_eq!(
            generic_compression_extension(&GenericCompressionStrategy::None),
            ""
        );
    }

    #[test]
    fn test_generic_compression_mime() {
        assert_eq!(
            generic_compression_mime(&GenericCompressionStrategy::Gzip),
            "application/gzip"
        );
        assert_eq!(
            generic_compression_mime(&GenericCompressionStrategy::Zstd),
            "application/zstd"
        );
        assert_eq!(
            generic_compression_mime(&GenericCompressionStrategy::Zip),
            "application/zip"
        );
        assert_eq!(
            generic_compression_mime(&GenericCompressionStrategy::SevenZ),
            "application/x-7z-compressed"
        );
        assert_eq!(
            generic_compression_mime(&GenericCompressionStrategy::OriginalFormat),
            ""
        );
        assert_eq!(
            generic_compression_mime(&GenericCompressionStrategy::None),
            ""
        );
    }

    #[test]
    fn test_mime_to_extension() {
        assert_eq!(mime_to_extension("image/webp"), Some("webp"));
        assert_eq!(mime_to_extension("image/avif"), Some("avif"));
        assert_eq!(mime_to_extension("video/mp4"), Some("mp4"));
        assert_eq!(mime_to_extension("application/gzip"), Some("gz"));
        assert_eq!(mime_to_extension("application/zstd"), Some("zst"));
        assert_eq!(mime_to_extension("application/zip"), Some("zip"));
        assert_eq!(mime_to_extension("application/x-7z-compressed"), Some("7z"));
        assert_eq!(mime_to_extension("application/x-rar"), Some("rar"));
        assert_eq!(mime_to_extension("unknown/type"), None);
        assert_eq!(mime_to_extension(""), None);
    }

    // ── Generic compression tests ──────────────────────────────────────────────

    fn create_compressible_data(dir: &std::path::Path) -> std::path::PathBuf {
        let input = dir.join("test.txt");
        // ~100KB of repetitive text that compresses very well
        let content =
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n".repeat(2000);
        std::fs::write(&input, content).unwrap();
        input
    }

    #[tokio::test]
    async fn test_compress_generic_gzip() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = create_compressible_data(&dir);

        let (path, size) = compress_generic_local(
            input.to_str().unwrap(),
            "test",
            &GenericCompressionStrategy::Gzip,
            5,
        )
        .await
        .unwrap();

        assert!(size > 0);
        assert_eq!(
            std::path::Path::new(&path)
                .extension()
                .map(|e| e.to_string_lossy()),
            Some("gz".into())
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_zstd() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = create_compressible_data(&dir);

        let (path, size) = compress_generic_local(
            input.to_str().unwrap(),
            "test",
            &GenericCompressionStrategy::Zstd,
            5,
        )
        .await
        .unwrap();

        assert!(size > 0);
        assert_eq!(
            std::path::Path::new(&path)
                .extension()
                .map(|e| e.to_string_lossy()),
            Some("zst".into())
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_zip() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = create_compressible_data(&dir);

        let (path, size) = compress_generic_local(
            input.to_str().unwrap(),
            "test",
            &GenericCompressionStrategy::Zip,
            5,
        )
        .await
        .unwrap();

        assert!(size > 0);
        assert_eq!(
            std::path::Path::new(&path)
                .extension()
                .map(|e| e.to_string_lossy()),
            Some("zip".into())
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_sevenz() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = create_compressible_data(&dir);

        let (path, size) = compress_generic_local(
            input.to_str().unwrap(),
            "test",
            &GenericCompressionStrategy::SevenZ,
            5,
        )
        .await
        .unwrap();

        assert!(size > 0);
        assert_eq!(
            std::path::Path::new(&path)
                .extension()
                .map(|e| e.to_string_lossy()),
            Some("7z".into())
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_original_format() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("test.txt");
        std::fs::write(&input, "Hello, world!").unwrap();
        let input_str = input.to_str().unwrap().to_string();

        let (path, size) = compress_generic_local(
            input_str.as_str(),
            "test",
            &GenericCompressionStrategy::OriginalFormat,
            5,
        )
        .await
        .unwrap();

        assert!(size > 0);
        assert_eq!(path, input_str);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_none() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("test.txt");
        std::fs::write(&input, "Hello, world!").unwrap();
        let input_str = input.to_str().unwrap().to_string();

        let (path, size) = compress_generic_local(
            input_str.as_str(),
            "test",
            &GenericCompressionStrategy::None,
            5,
        )
        .await
        .unwrap();

        assert!(size > 0);
        assert_eq!(path, input_str);
        std::fs::remove_dir_all(&dir).ok();
    }
}
