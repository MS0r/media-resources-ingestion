//! gRPC server: the `IngestService` implementation, the `serve()` entry
//! point, and the generated proto bindings.
//!
//! Callers reach this via `ingest_core::grpc::serve` and
//! `ingest_core::grpc::proto` (the latter is the tonic-generated module).

pub mod server;

pub use server::{IngestServer, proto, serve};
