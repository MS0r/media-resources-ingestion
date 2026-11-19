//! Worker: scheduler loop, job dispatch, shutdown handling.

pub mod shutdown;
pub mod worker;

pub use shutdown::Shutdown;
pub use worker::Worker;
