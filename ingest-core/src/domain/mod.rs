//! Pure domain types — no IO, no service dependencies.

pub mod headers;
pub mod source_auth;
pub mod status;

pub use headers::Headers;
pub use source_auth::SourceAuth;
pub use status::{JobStatus, JobStatusFilter};
