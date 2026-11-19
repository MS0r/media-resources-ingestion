//! Video compression strategies.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum VideoCompressionStrategy {
    #[default]
    H264,
    H265,
    Av1,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_h264() {
        assert_eq!(
            VideoCompressionStrategy::default(),
            VideoCompressionStrategy::H264
        );
    }

    #[test]
    fn test_serde_roundtrip() {
        for s in [
            VideoCompressionStrategy::H264,
            VideoCompressionStrategy::H265,
            VideoCompressionStrategy::Av1,
        ] {
            let json = serde_json::to_string(&s).unwrap();
            let back: VideoCompressionStrategy = serde_json::from_str(&json).unwrap();
            assert_eq!(back, s);
        }
    }
}
