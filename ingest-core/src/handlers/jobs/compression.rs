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
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc::{Receiver, Sender};

use std::{
    io,
    path::Path,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

use crate::{
    error::JobError,
    models::{GenericCompressionStrategy, ImageCompressionStrategy, VideoCompressionStrategy},
};

enum MuxItem {
    Video(frame::Video),
    Audio(frame::Audio),
    /// Compressed audio packet forwarded verbatim from input to output
    /// in the audio passthrough path (see `mp4_compatible_audio`).
    AudioPacket(Packet),
}

impl From<tokio::sync::mpsc::error::SendError<MuxItem>> for JobError {
    fn from(_: tokio::sync::mpsc::error::SendError<MuxItem>) -> Self {
        JobError::OtherFatal("Compression channel closed unexpectedly".into())
    }
}

/// Audio codec IDs that can be muxed into MP4 without re-encoding.
/// Source: ffmpeg `mov.c` `mov_known_audio_codecs` and the ISO/IEC 14496-12
/// sample entry boxes. Opus/FLAC/PCM-F64 are intentionally omitted for 2.1
/// (extradata/codec_tag polish is a follow-up).
pub(crate) fn mp4_compatible_audio(id: codec::Id) -> bool {
    use codec::Id;
    matches!(
        id,
        Id::AAC
            | Id::MP3
            | Id::AC3
            | Id::EAC3
            | Id::FLAC
            | Id::OPUS
            | Id::VORBIS
            | Id::PCM_S16BE
            | Id::PCM_S16LE
            | Id::PCM_S24BE
            | Id::PCM_S24LE
            | Id::PCM_S32BE
            | Id::PCM_S32LE
    )
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

/// Map a `GenericCompressionStrategy` to the canonical name used in
/// `Manifest.compression` (the inverse of `generic_compression_extension`
/// and `compress_generic_local`).
pub(crate) fn generic_compression_name(strategy: &GenericCompressionStrategy) -> &'static str {
    match strategy {
        GenericCompressionStrategy::Gzip => "gzip",
        GenericCompressionStrategy::Zstd => "zstd",
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
    quality: u8,
    temp_path: &str,
    strategy: &ImageCompressionStrategy,
) -> Result<(String, u64, String), JobError> {
    let original_name = original_name.to_string();
    let mime_type = mime_type.to_string();
    let temp_path = temp_path.to_string();
    let strategy = strategy.clone();

    tokio::task::spawn_blocking(move || {
        compress_image_local_inner(
            &original_name,
            &mime_type,
            original_size,
            quality,
            &temp_path,
            &strategy,
        )
    })
    .await
    .map_err(|e| JobError::OtherFatal(format!("Image compression join failed: {e}")))?
}

fn compress_image_local_inner(
    original_name: &str,
    mime_type: &str,
    original_size: u64,
    quality: u8,
    temp_path: &str,
    strategy: &ImageCompressionStrategy,
) -> Result<(String, u64, String), JobError> {
    use image::{
        ExtendedColorType, ImageEncoder, ImageFormat, ImageReader,
        codecs::{avif::AvifEncoder, webp::WebPEncoder},
    };
    use std::fs::File;
    use std::io::BufWriter;

    if mime_type == "image/webp"
        && matches!(
            strategy,
            ImageCompressionStrategy::Webp | ImageCompressionStrategy::LosslessWebp
        )
    {
        let meta = std::fs::metadata(temp_path)?;
        return Ok((temp_path.to_string(), meta.len(), mime_type.to_string()));
    }

    let format = match mime_type {
        "image/jpeg" | "image/jpg" => ImageFormat::Jpeg,
        "image/png" => ImageFormat::Png,
        "image/gif" => ImageFormat::Gif,
        "image/webp" => ImageFormat::WebP,
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
    // `image 0.25` only exposes a lossless WebP encoder
    // (`WebPEncoder::new_lossless`); there is no public lossy path.
    // Both `Webp` and `LosslessWebp` use it. The `Webp` variant is
    // retained for backward compatibility with existing YAML files.
    match strategy {
        ImageCompressionStrategy::Avif => {
            let file = File::create(&output_path)?;
            // speed 1-10 (1 slowest, 10 fastest); 4 is "balanced".
            // quality 1-100 (1 worst, 100 best); 0 is a sentinel that the
            // encoder rejects, so we floor at 1.
            AvifEncoder::new_with_speed_quality(BufWriter::new(file), 4, quality.max(1))
                .write_image(rgba.as_raw(), width, height, ExtendedColorType::Rgba8)?;
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
    // If the input audio codec is already MP4-native, we copy the stream
    // through verbatim (no decode/encode) to avoid quality loss from
    // re-encoding. Otherwise (or when there is no audio), we keep the
    // existing AAC re-encode path.
    let audio_passthrough = audio_decoder
        .as_ref()
        .map(|d| mp4_compatible_audio(d.id()))
        .unwrap_or(false);

    if let Some(ref mut audio_dec) = audio_decoder {
        if audio_passthrough {
            // Set up the output stream from the input stream's codec params.
            // `set_parameters` calls `avcodec_parameters_copy`, which copies
            // codec_id, sample_rate, channels, channel_layout, and extradata.
            // 2.2 will add explicit codec_tag handling for AC3/EAC3/FLAC/Opus.
            let input_audio_stream = audio_stream_index
                .and_then(|idx| ictx.stream(idx))
                .ok_or_else(|| {
                    JobError::OtherFatal("Missing input audio stream for passthrough".into())
                })?;
            let in_codec_id = input_audio_stream.parameters().id();
            let in_codec = encoder::find(in_codec_id).ok_or_else(|| {
                JobError::OtherFatal(format!(
                    "Codec {in_codec_id:?} not available for output muxer"
                ))
            })?;
            let mut ost = octx.add_stream(in_codec)?;
            ost.set_parameters(input_audio_stream.parameters());
            ost.set_time_base(input_audio_stream.time_base());
            audio_ost_index = Some(octx.streams().count() - 1);
            // audio_encoder and audio_filter stay None: the encode loop
            // sees no reencode-side state and remuxes the packets
            // directly via MuxItem::AudioPacket.
        } else {
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
                    .ok_or_else(|| {
                        JobError::OtherFatal("AAC encoder has no supported formats".into())
                    })?
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
    }

    octx.set_metadata(ictx.metadata().to_owned());
    octx.write_header()?;

    if let Some(ost_idx) = audio_ost_index {
        audio_ostb = Some(
            octx.stream(ost_idx)
                .ok_or_else(|| JobError::OtherFatal("Missing audio output stream".into()))?
                .time_base(),
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
            audio_passthrough,
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
    audio_passthrough: bool,
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
            && stream_idx == audio_idx
        {
            if audio_passthrough {
                // Forward the compressed packet verbatim. The encoder
                // never sees audio frames, so we never feed the audio
                // decoder either; the post-loop decoder flush is a
                // harmless no-op for that reason.
                tx.blocking_send(MuxItem::AudioPacket(packet))?;
            } else if let Some(ref mut audio_dec) = audio_decoder {
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

    // Flush audio decoder — only needed when re-encoding. In passthrough
    // mode the decoder never received any packets, so the flush is a
    // no-op, but we still skip it explicitly to keep the intent clear.
    if !audio_passthrough && let Some(ref mut audio_dec) = audio_decoder {
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
            MuxItem::AudioPacket(mut packet) => {
                if let Some(ast_idx) = audio_ost_index {
                    packet.set_stream(ast_idx);
                    // No-op when input and output time bases match (the
                    // common case after `set_time_base(input.time_base())`
                    // on the output stream). If they differ, this is the
                    // correct remux-time conversion.
                    packet.rescale_ts(audio_istb, audio_ostb);
                    packet.write_interleaved(octx)?;
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
    cancelled: Arc<AtomicBool>,
) -> Result<(String, u64, GenericCompressionStrategy), JobError> {
    match strategy {
        GenericCompressionStrategy::OriginalFormat | GenericCompressionStrategy::None => {
            let meta = std::fs::metadata(temp_path)?;
            return Ok((temp_path.to_string(), meta.len(), strategy.clone()));
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
    let applied = strategy.clone();
    let applied_for_return = applied.clone();
    let cancelled = cancelled.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<(String, u64), JobError> {
        // Map the public 0..=100 `quality` knob onto each algorithm's native
        // level scale: gzip=1..=9, zip=1..=9 (deflate), 7z=1..=9 (LZMA2 preset),
        // zstd=1..=22 (already a per-strategy line in its own arm). We floor
        // at 1 because level 0 in deflate is "store" and would actually grow
        // small inputs; "quality=0" should still compress, just at the
        // fastest setting.
        let level_0_9 = ((quality.max(1) as u32 * 9) / 100).clamp(1, 9);

        // Helper: cheap pre-check for cancellation. The actual `io::copy`
        // inside each arm is a single synchronous call and cannot be
        // interrupted mid-stream; if the flag flips during the copy, the
        // next call into `compress_generic_local` (or, for 7z, the next
        // call into `push_source_path`'s filter) will observe it.
        let check_cancel = || -> Result<(), JobError> {
            if cancelled.load(Ordering::Relaxed) {
                Err(JobError::OtherFatal("Generic compression cancelled".into()))
            } else {
                Ok(())
            }
        };

        match &applied {
            GenericCompressionStrategy::Gzip => {
                check_cancel()?;
                let mut input = std::fs::File::open(&input_path)?;
                let output = std::fs::File::create(&out_path)?;
                let mut encoder =
                    flate2::write::GzEncoder::new(output, flate2::Compression::new(level_0_9));
                io::copy(&mut input, &mut encoder)?;
                encoder.finish()?;
            }
            GenericCompressionStrategy::Zstd => {
                check_cancel()?;
                let mut input = std::fs::File::open(&input_path)?;
                let output = std::fs::File::create(&out_path)?;
                let level = (quality as i32).clamp(1, 22);
                let mut encoder = zstd::stream::Encoder::new(output, level)?;
                io::copy(&mut input, &mut encoder)?;
                encoder.finish()?;
            }
            GenericCompressionStrategy::Zip => {
                check_cancel()?;
                let output = std::fs::File::create(&out_path)?;
                let mut zip = zip::ZipWriter::new(output);
                let fname = Path::new(&input_path)
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "file".to_string());
                let opts = zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Deflated)
                    .compression_level(Some(level_0_9 as i64));
                zip.start_file(fname, opts)?;
                let mut input = std::fs::File::open(&input_path)?;
                io::copy(&mut input, &mut zip)?;
                zip.finish()?;
            }
            GenericCompressionStrategy::SevenZ => {
                check_cancel()?;
                let mut writer = sevenz_rust::SevenZWriter::create(&out_path)?;
                // LZMA2 preset 0..=9. `sevenz_rust::lzma::LZMA2Options` is
                // re-exported from `lzma_rust` and is the only knob
                // `set_content_methods` accepts for the LZMA2 method.
                let lzma2_opts = sevenz_rust::lzma::LZMA2Options::with_preset(level_0_9);
                writer.set_content_methods(vec![sevenz_rust::SevenZMethodConfiguration::from(
                    lzma2_opts,
                )]);
                // The filter callback runs once per file. Returning false
                // skips the file, which is the best cancellation hook the
                // `sevenz-rust 0.6.1` API offers.
                writer.push_source_path(Path::new(&input_path), |_| {
                    !cancelled.load(Ordering::Relaxed)
                })?;
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
        return Ok((
            temp_path.to_string(),
            original_size,
            GenericCompressionStrategy::OriginalFormat,
        ));
    }

    Ok((result.0, result.1, applied_for_return))
}

/// `AsyncRead` adapter that pumps a synchronous `Read` (driven on a
/// blocking task) into an async consumer via a bounded mpsc channel.
///
/// The producer side owns a synchronous reader, reads up to 64 KiB chunks,
/// and `blocking_send`s them to the consumer. The consumer side returns
/// `Pending` while it waits for the next chunk. Dropping the reader
/// unblocks the producer (its `blocking_send` returns `Err`).
pub(crate) struct BlockingReader {
    rx: tokio::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>,
    current: Vec<u8>,
    current_pos: usize,
    finished: bool,
}

impl BlockingReader {
    fn new(rx: tokio::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>) -> Self {
        Self {
            rx,
            current: Vec::new(),
            current_pos: 0,
            finished: false,
        }
    }
}

impl AsyncRead for BlockingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            // Drain whatever we have buffered.
            if self.current_pos < self.current.len() {
                let remaining = self.current.len() - self.current_pos;
                let to_copy = remaining.min(buf.remaining());
                buf.put_slice(&self.current[self.current_pos..self.current_pos + to_copy]);
                self.current_pos += to_copy;
                return Poll::Ready(Ok(()));
            }
            if self.finished {
                return Poll::Ready(Ok(()));
            }
            // No buffered bytes — try to receive the next chunk.
            match Pin::new(&mut self.rx).poll_recv(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    self.current = bytes;
                    self.current_pos = 0;
                    // Loop and try to fill the caller's buffer.
                }
                Poll::Ready(Some(Err(e))) => {
                    self.finished = true;
                    return Poll::Ready(Err(e));
                }
                Poll::Ready(None) => {
                    self.finished = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Stream-decompress the bytes produced by `input` using the named
/// strategy. `max_bytes` truncates the output at the given length (0
/// means no limit). The returned `AsyncRead` is `Send + Unpin` so it
/// composes with the rest of the pipeline.
///
/// Internally spawns a `spawn_blocking` task that:
/// 1. Wraps the async input in `SyncIoBridge` to present it as `Read`.
/// 2. Wraps that in the appropriate sync decompressor (gzip, zstd, zip, 7z).
/// 3. `blocking_send`s 64 KiB chunks to the consumer.
///
/// For seek-required formats (zip, 7z) the compressed input is buffered
/// into a `Vec<u8>` first; the format overhead on the buffer is small
/// because those formats compress better than gzip.
///
/// The channel is bounded (capacity 8) — backpressure flows naturally
/// from the consumer's `AsyncRead::poll_read` returning `Pending`.
pub(crate) fn decompress_generic_reader(
    mut input: Box<dyn AsyncRead + Unpin + Send>,
    compression: &str,
    max_bytes: u64,
) -> Pin<Box<dyn AsyncRead + Send>> {
    let compression = compression.to_string();
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<Vec<u8>>>(8);

    let needs_seek = matches!(compression.as_str(), "zip" | "7z");
    let tx_clone = tx.clone();
    let compression_clone = compression.clone();

    if needs_seek {
        // Buffer the async input, then hand it to a blocking task that
        // runs the seek-required decompressor over the buffer.
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut buffered = Vec::new();
            let result = match input.read_to_end(&mut buffered).await {
                Ok(_) => {
                    let tx_inner = tx_clone.clone();
                    let compression_inner = compression_clone.clone();
                    tokio::task::spawn_blocking(move || {
                        let result =
                            decompress_buffered(&compression_inner, buffered, max_bytes, &tx_inner);
                        if let Err(e) = result {
                            let _ = tx_inner.blocking_send(Err(e));
                        }
                    });
                    Ok(())
                }
                Err(e) => Err(e),
            };
            if let Err(e) = result {
                let _ = tx_clone.send(Err(e)).await;
            }
        });
    } else {
        // Streaming path — wrap the async input in SyncIoBridge inside a
        // blocking task.
        tokio::task::spawn_blocking(move || {
            use tokio_util::io::SyncIoBridge;
            let sync_input = SyncIoBridge::new(input);
            let result = decompress_streaming(sync_input, &compression_clone, max_bytes, &tx);
            if let Err(e) = result {
                let _ = tx.blocking_send(Err(e));
            }
        });
    }

    let reader = BlockingReader::new(rx);
    Box::pin(reader)
}

/// Streaming decompression — used for formats that don't need `Seek`
/// (gzip, zstd, none, unknown passthrough).
fn decompress_streaming<R: std::io::Read + Send + 'static>(
    sync_input: R,
    compression: &str,
    max_bytes: u64,
    tx: &tokio::sync::mpsc::Sender<std::io::Result<Vec<u8>>>,
) -> std::io::Result<()> {
    use std::io::Read;
    let mut reader: Box<dyn Read + Send> = match compression {
        "gzip" => Box::new(flate2::read::GzDecoder::new(sync_input)),
        "zstd" => Box::new(zstd::stream::Decoder::new(sync_input)?),
        "" | "none" | "originalformat" => Box::new(sync_input),
        other => {
            tracing::warn!(
                compression = %other,
                "Unknown chunk compression strategy; passing bytes through"
            );
            Box::new(sync_input)
        }
    };

    let mut buf = vec![0u8; 64 * 1024];
    let mut sent: u64 = 0;
    loop {
        if max_bytes > 0 && sent >= max_bytes {
            return Ok(());
        }
        let n = reader.read(&mut buf)?;
        if n == 0 {
            return Ok(());
        }
        let to_send = if max_bytes > 0 {
            let allowed = (max_bytes - sent) as usize;
            n.min(allowed)
        } else {
            n
        };
        if to_send == 0 {
            return Ok(());
        }
        if tx.blocking_send(Ok(buf[..to_send].to_vec())).is_err() {
            return Ok(()); // consumer dropped
        }
        sent += to_send as u64;
    }
}

/// Buffered decompression — used for formats that need `Seek` (zip, 7z).
fn decompress_buffered(
    compression: &str,
    buffered: Vec<u8>,
    max_bytes: u64,
    tx: &tokio::sync::mpsc::Sender<std::io::Result<Vec<u8>>>,
) -> std::io::Result<()> {
    use std::io::Read;
    let len = buffered.len() as u64;
    let cursor = std::io::Cursor::new(buffered);

    match compression {
        "zip" => {
            let mut archive = zip::ZipArchive::new(cursor)?;
            if archive.len() == 0 {
                return Ok(());
            }
            let file: zip::read::ZipFile<'_, std::io::Cursor<Vec<u8>>> = archive.by_index(0)?;
            // Always wrap in `Take` so the types are uniform; `u64::MAX`
            // means "no limit".
            let limit = if max_bytes > 0 { max_bytes } else { u64::MAX };
            let mut limited = file.take(limit);
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = limited.read(&mut buf)?;
                if n == 0 {
                    return Ok(());
                }
                if tx.blocking_send(Ok(buf[..n].to_vec())).is_err() {
                    return Ok(());
                }
            }
        }
        "7z" => {
            let mut reader =
                sevenz_rust::SevenZReader::new(cursor, len, sevenz_rust::Password::empty())
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                    })?;
            let mut sent: u64 = 0;
            let mut buf = vec![0u8; 64 * 1024];
            reader
                .for_each_entries(|_entry, entry_reader| {
                    loop {
                        if max_bytes > 0 && sent >= max_bytes {
                            return Ok(false);
                        }
                        let n = entry_reader.read(&mut buf)?;
                        if n == 0 {
                            return Ok(true);
                        }
                        let to_send = if max_bytes > 0 {
                            let allowed = (max_bytes - sent) as usize;
                            n.min(allowed)
                        } else {
                            n
                        };
                        if to_send == 0 {
                            return Ok(false);
                        }
                        if tx.blocking_send(Ok(buf[..to_send].to_vec())).is_err() {
                            return Ok(false);
                        }
                        sent += to_send as u64;
                    }
                })
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            Ok(())
        }
        _ => unreachable!("decompress_buffered called for non-seek strategy: {compression}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::GenericCompressionStrategy;
    use sha2::Digest;

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

    #[test]
    fn test_mp4_compatible_audio() {
        use ffmpeg_next::codec::Id;
        // Common MP4-native codecs
        assert!(mp4_compatible_audio(Id::AAC));
        assert!(mp4_compatible_audio(Id::MP3));
        assert!(mp4_compatible_audio(Id::AC3));
        assert!(mp4_compatible_audio(Id::EAC3));
        assert!(mp4_compatible_audio(Id::FLAC));
        assert!(mp4_compatible_audio(Id::OPUS));
        assert!(mp4_compatible_audio(Id::VORBIS));
        // PCM variants we accept
        assert!(mp4_compatible_audio(Id::PCM_S16LE));
        assert!(mp4_compatible_audio(Id::PCM_S24BE));
        assert!(mp4_compatible_audio(Id::PCM_S32LE));
        // Not in the allowlist: these need AAC transcoding
        assert!(!mp4_compatible_audio(Id::None));
        assert!(!mp4_compatible_audio(Id::TRUEHD));
        assert!(!mp4_compatible_audio(Id::ALAC));
        assert!(!mp4_compatible_audio(Id::WMAV1));
        assert!(!mp4_compatible_audio(Id::WMAV2));
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

        let (path, size, applied) = compress_generic_local(
            input.to_str().unwrap(),
            "test",
            &GenericCompressionStrategy::Gzip,
            5,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
        assert_eq!(applied, GenericCompressionStrategy::Gzip);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_zstd() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = create_compressible_data(&dir);

        let (path, size, applied) = compress_generic_local(
            input.to_str().unwrap(),
            "test",
            &GenericCompressionStrategy::Zstd,
            5,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
        assert_eq!(applied, GenericCompressionStrategy::Zstd);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_zip() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = create_compressible_data(&dir);

        let (path, size, applied) = compress_generic_local(
            input.to_str().unwrap(),
            "test",
            &GenericCompressionStrategy::Zip,
            5,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
        assert_eq!(applied, GenericCompressionStrategy::Zip);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_sevenz() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = create_compressible_data(&dir);

        let (path, size, applied) = compress_generic_local(
            input.to_str().unwrap(),
            "test",
            &GenericCompressionStrategy::SevenZ,
            5,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
        assert_eq!(applied, GenericCompressionStrategy::SevenZ);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── Quality-level monotonicity tests ───────────────────────────────────────

    /// ~512 KiB of structured but repetitive text that exercises the level
    /// difference for DEFLATE/LZMA2 (repetitive patterns compress well at
    /// every level, but higher levels reduce the output further).
    fn create_stress_data(dir: &std::path::Path) -> std::path::PathBuf {
        let line = "The quick brown fox jumps over the lazy dog. \
                    Pack my box with five dozen liquor jugs. 0123456789.\n";
        let body = line.repeat(8 * 1024); // ~512 KiB
        let input = dir.join("stress.txt");
        std::fs::write(&input, body).unwrap();
        input
    }

    async fn compress_at_quality(
        dir: &std::path::Path,
        strategy: GenericCompressionStrategy,
        quality: u8,
    ) -> u64 {
        // Each call gets its own copy of the stress data because
        // `compress_generic_local` deletes the input on success.
        let src = dir.join(format!("q{}_{}.txt", quality, uuid::Uuid::new_v4()));
        std::fs::copy(dir.join("stress.txt"), &src).unwrap();
        let (out, size, _applied) = compress_generic_local(
            src.to_str().unwrap(),
            "stress",
            &strategy,
            quality,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .await
        .unwrap();
        assert!(size > 0, "{strategy:?}@q{quality} produced empty output");
        // The returned path may be either the compressed .gz/.zip/.7z file
        // (smaller) or the original (when not smaller). Either way, `size`
        // is the size we kept.
        let _ = out;
        size
    }

    #[tokio::test]
    async fn test_compress_generic_gzip_levels_differ() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        create_stress_data(&dir);

        let low = compress_at_quality(&dir, GenericCompressionStrategy::Gzip, 10).await;
        let high = compress_at_quality(&dir, GenericCompressionStrategy::Gzip, 95).await;
        // Higher quality (level 9) must not be larger than lower quality
        // (level 1) on a compressible payload.
        assert!(high <= low, "gzip q95 ({high}) > q10 ({low})");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_zip_levels_differ() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        create_stress_data(&dir);

        let low = compress_at_quality(&dir, GenericCompressionStrategy::Zip, 10).await;
        let high = compress_at_quality(&dir, GenericCompressionStrategy::Zip, 95).await;
        assert!(high <= low, "zip q95 ({high}) > q10 ({low})");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    #[ignore = "LZMA2 preset sweep on 512 KiB takes ~5-10s; run with --ignored"]
    async fn test_compress_generic_sevenz_levels_differ() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        create_stress_data(&dir);

        let low = compress_at_quality(&dir, GenericCompressionStrategy::SevenZ, 10).await;
        let high = compress_at_quality(&dir, GenericCompressionStrategy::SevenZ, 95).await;
        assert!(high <= low, "7z q95 ({high}) > q10 ({low})");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_original_format() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("test.txt");
        std::fs::write(&input, "Hello, world!").unwrap();
        let input_str = input.to_str().unwrap().to_string();

        let (path, size, applied) = compress_generic_local(
            input_str.as_str(),
            "test",
            &GenericCompressionStrategy::OriginalFormat,
            5,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .await
        .unwrap();

        assert!(size > 0);
        assert_eq!(path, input_str);
        assert_eq!(applied, GenericCompressionStrategy::OriginalFormat);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_none() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("test.txt");
        std::fs::write(&input, "Hello, world!").unwrap();
        let input_str = input.to_str().unwrap().to_string();

        let (path, size, applied) = compress_generic_local(
            input_str.as_str(),
            "test",
            &GenericCompressionStrategy::None,
            5,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .await
        .unwrap();

        assert!(size > 0);
        assert_eq!(path, input_str);
        assert_eq!(applied, GenericCompressionStrategy::None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_generic_incompressible_falls_back_to_original() {
        // 1 MiB of SHA-256 counter output — incompressible. Gzip would
        // grow the file, so compress_generic_local should fall back to
        // OriginalFormat and return the input path (not the .gz path).
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("random.bin");
        let mut data = Vec::with_capacity(1024 * 1024);
        for i in 0u32..32768 {
            let mut hasher = sha2::Sha256::new();
            hasher.update(i.to_le_bytes());
            data.extend_from_slice(&hasher.finalize());
        }
        std::fs::write(&input, &data).unwrap();
        let input_str = input.to_str().unwrap().to_string();

        let (path, size, applied) = compress_generic_local(
            input_str.as_str(),
            "random",
            &GenericCompressionStrategy::Gzip,
            5,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .await
        .unwrap();

        // Size must equal the original — no compression was applied.
        assert_eq!(size, data.len() as u64);
        // Path must be the original input (not .gz) — the .gz was deleted.
        assert_eq!(path, input_str);
        // The applied strategy must reflect the actual outcome.
        assert_eq!(applied, GenericCompressionStrategy::OriginalFormat);
        // The .gz output file should not exist.
        assert!(!std::path::Path::new(&format!("{input_str}.gz")).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── Image compression tests ─────────────────────────────────────────────────

    /// Build a noisy 256x256 RGBA8 image as a PNG on disk and return the path.
    /// Uses a deterministic pseudo-random gradient so the AVIF encoder has
    /// something to actually compress at every quality level.
    fn write_test_png(dir: &std::path::Path) -> std::path::PathBuf {
        use image::{ImageBuffer, Rgba};
        let mut img: ImageBuffer<Rgba<u8>, Vec<u8>> = ImageBuffer::new(256, 256);
        for y in 0u32..256 {
            for x in 0u32..256 {
                let r = ((x.wrapping_mul(37) ^ y.wrapping_mul(53)) & 0xff) as u8;
                let g = ((x.wrapping_add(y).wrapping_mul(17)) & 0xff) as u8;
                let b = ((x.wrapping_sub(y).wrapping_mul(29)) & 0xff) as u8;
                img.put_pixel(x, y, Rgba([r, g, b, 255]));
            }
        }
        let path = dir.join("test.png");
        img.save(&path).unwrap();
        path
    }

    /// Build a 256x256 WebP file on disk and return the path. Uses a smooth
    /// gradient so that AVIF (which excels on natural-ish content) can
    /// actually win against the lossless WebP source.
    fn write_test_webp(dir: &std::path::Path) -> std::path::PathBuf {
        use image::{ExtendedColorType, ImageEncoder, codecs::webp::WebPEncoder};
        let mut rgba: Vec<u8> = Vec::with_capacity(256 * 256 * 4);
        for y in 0u32..256 {
            for x in 0u32..256 {
                rgba.push((x & 0xff) as u8);
                rgba.push((y & 0xff) as u8);
                rgba.push(((x.wrapping_add(y) / 2) & 0xff) as u8);
                rgba.push(255);
            }
        }
        let path = dir.join("test.webp");
        let f = std::fs::File::create(&path).unwrap();
        WebPEncoder::new_lossless(std::io::BufWriter::new(f))
            .write_image(&rgba, 256, 256, ExtendedColorType::Rgba8)
            .unwrap();
        path
    }

    #[tokio::test]
    async fn test_compress_image_avif_quality_round_trip() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let original = write_test_png(&dir);
        let input_size = std::fs::metadata(&original).unwrap().len() as u64;

        // `compress_image_local` deletes `temp_path` on success, so copy
        // the fixture for the second pass.
        let copy_a = dir.join("a.png");
        let copy_b = dir.join("b.png");
        std::fs::copy(&original, &copy_a).unwrap();
        std::fs::copy(&original, &copy_b).unwrap();

        let (_path30, size30, _mime30) = compress_image_local(
            "test",
            "image/png",
            input_size,
            30,
            copy_a.to_str().unwrap(),
            &ImageCompressionStrategy::Avif,
        )
        .await
        .unwrap();
        let (_path90, size90, _mime90) = compress_image_local(
            "test",
            "image/png",
            input_size,
            90,
            copy_b.to_str().unwrap(),
            &ImageCompressionStrategy::Avif,
        )
        .await
        .unwrap();

        // Lower quality should not be larger than higher quality.
        assert!(size30 <= size90, "q30 ({}) > q90 ({})", size30, size90);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_image_webp_source_keeps_with_webp_strategy() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = write_test_webp(&dir);
        let input_size = std::fs::metadata(&input).unwrap().len() as u64;

        // WebP source + LosslessWebp strategy → unchanged, no re-encode.
        let (path, size, mime) = compress_image_local(
            "test",
            "image/webp",
            input_size,
            80,
            input.to_str().unwrap(),
            &ImageCompressionStrategy::LosslessWebp,
        )
        .await
        .unwrap();
        assert_eq!(path, input.to_string_lossy());
        assert_eq!(size, input_size);
        assert_eq!(mime, "image/webp");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_compress_image_webp_source_with_avif_strategy() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let input = write_test_webp(&dir);
        let input_size = std::fs::metadata(&input).unwrap().len() as u64;

        let (path, size, mime) = compress_image_local(
            "test",
            "image/webp",
            input_size,
            60,
            input.to_str().unwrap(),
            &ImageCompressionStrategy::Avif,
        )
        .await
        .unwrap();
        assert!(path.ends_with(".avif"), "expected .avif, got {path}");
        assert!(size > 0);
        assert_eq!(mime, "image/avif");

        std::fs::remove_dir_all(&dir).ok();
    }

    // ── Generic decompression tests ──────────────────────────────────────────────

    use tokio::io::AsyncReadExt;

    /// Run an `AsyncRead` to completion, returning the bytes read.
    async fn read_all(mut r: Pin<Box<dyn AsyncRead + Send>>) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::new();
        r.read_to_end(&mut out).await?;
        Ok(out)
    }

    /// Compress `data` with gzip at level 6, return the encoded bytes.
    fn gzip_encode(data: &[u8]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;
        let mut enc = GzEncoder::new(Vec::new(), Compression::new(6));
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    /// Compress `data` with zstd at level 3, return the encoded bytes.
    fn zstd_encode(data: &[u8]) -> Vec<u8> {
        zstd::stream::encode_all(data, 3).unwrap()
    }

    /// Compress `data` as a single-entry zip, return the encoded bytes.
    fn zip_encode(data: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated)
                .compression_level(Some(6));
            zw.start_file("payload.bin", opts).unwrap();
            zw.write_all(data).unwrap();
            zw.finish().unwrap();
        }
        buf
    }

    /// Compress `data` as a 7z archive, return the encoded bytes.
    fn sevenz_encode(data: &[u8]) -> Vec<u8> {
        let tmp_in = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::write(&tmp_in, data).unwrap();
        let tmp_out = std::env::temp_dir().join(format!("{}.7z", uuid::Uuid::new_v4()));
        {
            let mut szw = sevenz_rust::SevenZWriter::create(&tmp_out).unwrap();
            let lzma2_opts = sevenz_rust::lzma::LZMA2Options::with_preset(6);
            szw.set_content_methods(vec![sevenz_rust::SevenZMethodConfiguration::from(
                lzma2_opts,
            )]);
            szw.push_source_path(&tmp_in, |_| true).unwrap();
            szw.finish().unwrap();
        }
        let buf = std::fs::read(&tmp_out).unwrap();
        std::fs::remove_file(&tmp_in).ok();
        std::fs::remove_file(&tmp_out).ok();
        buf
    }

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn test_decompress_generic_gzip_roundtrip() {
        let original: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
        let encoded = gzip_encode(&original);
        let input: Box<dyn AsyncRead + Unpin + Send> = Box::new(std::io::Cursor::new(encoded));
        let reader = decompress_generic_reader(input, "gzip", original.len() as u64);
        let out = read_all(reader).await.unwrap();
        assert_eq!(out, original);
    }

    #[tokio::test]
    async fn test_decompress_generic_zstd_roundtrip() {
        let original: Vec<u8> = (0..128 * 1024u32)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect();
        let encoded = zstd_encode(&original);
        let input: Box<dyn AsyncRead + Unpin + Send> = Box::new(std::io::Cursor::new(encoded));
        let reader = decompress_generic_reader(input, "zstd", original.len() as u64);
        let out = read_all(reader).await.unwrap();
        assert_eq!(out, original);
    }

    #[tokio::test]
    async fn test_decompress_generic_zip_roundtrip() {
        let original: Vec<u8> = (0..32 * 1024u32)
            .map(|i| (i.wrapping_mul(13) % 251) as u8)
            .collect();
        let encoded = zip_encode(&original);
        let input: Box<dyn AsyncRead + Unpin + Send> = Box::new(std::io::Cursor::new(encoded));
        let reader = decompress_generic_reader(input, "zip", original.len() as u64);
        let out = read_all(reader).await.unwrap();
        assert_eq!(out, original);
    }

    #[tokio::test]
    async fn test_decompress_generic_7z_roundtrip() {
        let original: Vec<u8> = (0..16 * 1024u32)
            .map(|i| (i.wrapping_mul(7) % 251) as u8)
            .collect();
        let encoded = sevenz_encode(&original);
        let input: Box<dyn AsyncRead + Unpin + Send> = Box::new(std::io::Cursor::new(encoded));
        let reader = decompress_generic_reader(input, "7z", original.len() as u64);
        let out = read_all(reader).await.unwrap();
        assert_eq!(out, original);
    }

    #[tokio::test]
    async fn test_decompress_generic_passthrough() {
        let original: Vec<u8> = b"hello world, no compression here".to_vec();
        let input: Box<dyn AsyncRead + Unpin + Send> =
            Box::new(std::io::Cursor::new(original.clone()));
        let reader = decompress_generic_reader(input, "", 0);
        let out = read_all(reader).await.unwrap();
        assert_eq!(out, original);
    }

    #[tokio::test]
    async fn test_decompress_generic_unknown_strategy_passthrough() {
        let original: Vec<u8> = b"mystery bytes".to_vec();
        let input: Box<dyn AsyncRead + Unpin + Send> =
            Box::new(std::io::Cursor::new(original.clone()));
        let reader = decompress_generic_reader(input, "not-a-real-codec", 0);
        let out = read_all(reader).await.unwrap();
        assert_eq!(out, original);
    }

    #[tokio::test]
    async fn test_decompress_generic_max_bytes_truncates() {
        let original: Vec<u8> = (0..8192).map(|i| (i % 251) as u8).collect();
        let encoded = gzip_encode(&original);
        let input: Box<dyn AsyncRead + Unpin + Send> = Box::new(std::io::Cursor::new(encoded));
        let reader = decompress_generic_reader(input, "gzip", 100);
        let out = read_all(reader).await.unwrap();
        assert_eq!(out.len(), 100);
        assert_eq!(out, &original[..100]);
    }

    #[tokio::test]
    async fn test_decompress_generic_from_file() {
        // End-to-end: write a gzipped file to disk, decompress via the reader.
        let original: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let encoded = gzip_encode(&original);
        let dir = temp_dir();
        let path = dir.join("payload.bin.gz");
        std::fs::write(&path, &encoded).unwrap();

        let file = tokio::fs::File::open(&path).await.unwrap();
        let input: Box<dyn AsyncRead + Unpin + Send> = Box::new(file);
        let reader = decompress_generic_reader(input, "gzip", original.len() as u64);
        let out = read_all(reader).await.unwrap();
        assert_eq!(out, original);

        std::fs::remove_dir_all(&dir).ok();
    }
}
