//! Image compression strategies.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ImageCompressionStrategy {
    #[default]
    Avif,
    // NOTE: in `image 0.25` both `Webp` and `LosslessWebp` resolve to
    // the same lossless encoder. `Webp` is kept for backward
    // compatibility with existing YAML files and may be wired to a
    // lossy path if/when the `webp` crate is added.
    Webp,
    LosslessWebp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_avif() {
        assert_eq!(
            ImageCompressionStrategy::default(),
            ImageCompressionStrategy::Avif
        );
    }

    #[test]
    fn test_serde_roundtrip() {
        for s in [
            ImageCompressionStrategy::Avif,
            ImageCompressionStrategy::Webp,
            ImageCompressionStrategy::LosslessWebp,
        ] {
            let json = serde_json::to_string(&s).unwrap();
            let back: ImageCompressionStrategy = serde_json::from_str(&json).unwrap();
            assert_eq!(back, s);
        }
    }
}
