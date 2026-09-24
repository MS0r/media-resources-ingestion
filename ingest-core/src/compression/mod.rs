//! Compression strategies and execution.
//!
//! - [`generic`], [`image`], [`video`] — strategy enums plus the
//!   [`generic::CompressionOverride`] umbrella that the YAML schema picks
//!   from.
//! - [`plan`] — `decide()` / `apply()` helpers that turn an override +
//!   detected MIME into a concrete compression result.

pub mod generic;
pub mod image;
pub mod plan;
pub mod video;

pub use generic::{CompressionOverride, GenericCompressionStrategy, UniversalCompressionStrategy};
pub use image::ImageCompressionStrategy;
pub use plan::{AppliedCompression, CompressionPlan, decide, default_chunk_compression};
pub use video::VideoCompressionStrategy;
