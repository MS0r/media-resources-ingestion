//! Generic (archive/byte-stream) compression strategies and the
//! `CompressionOverride` umbrella enum that picks one of the three
//! families (image / video / generic / universal).

use serde::{Deserialize, Serialize};

pub use super::image::ImageCompressionStrategy;
pub use super::video::VideoCompressionStrategy;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum GenericCompressionStrategy {
    #[default]
    OriginalFormat,
    Gzip,
    Zstd,
    Zip,
    SevenZ,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum UniversalCompressionStrategy {
    None,
}

/// User-supplied compression override on a YAML resource or the ingestion
/// root. `Universal` is the legacy catch-all (effectively passthrough).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CompressionOverride {
    Image(ImageCompressionStrategy),
    Video(VideoCompressionStrategy),
    Generic(GenericCompressionStrategy),
    Universal(UniversalCompressionStrategy),
}

impl Default for CompressionOverride {
    fn default() -> Self {
        CompressionOverride::Universal(UniversalCompressionStrategy::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_universal_none() {
        assert_eq!(
            CompressionOverride::default(),
            CompressionOverride::Universal(UniversalCompressionStrategy::None)
        );
    }

    #[test]
    fn test_generic_default_is_original_format() {
        assert_eq!(
            GenericCompressionStrategy::default(),
            GenericCompressionStrategy::OriginalFormat
        );
    }

    #[test]
    fn test_generic_serde_lowercase() {
        let s = GenericCompressionStrategy::Gzip;
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, "\"gzip\"");
    }

    #[test]
    fn test_compression_override_untagged_dispatch() {
        // serde untagged picks the first variant that deserializes successfully.
        let img: CompressionOverride = serde_json::from_str("\"avif\"").unwrap();
        assert!(matches!(
            img,
            CompressionOverride::Image(ImageCompressionStrategy::Avif)
        ));

        let generic: CompressionOverride = serde_json::from_str("\"gzip\"").unwrap();
        assert!(matches!(
            generic,
            CompressionOverride::Generic(GenericCompressionStrategy::Gzip)
        ));
    }
}
