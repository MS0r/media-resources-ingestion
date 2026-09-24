//! Single SIGINT handler — replaces duplicate handlers in `server.rs` and `bootstrap.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::services::redis::RedisService;

/// Graceful shutdown handle — shared between the gRPC server and the worker.
pub struct Shutdown {
    flag: Arc<AtomicBool>,
}

impl Shutdown {
    /// Install a single SIGINT handler that:
    /// 1. Clears all `jobs:running:*` keys in Redis
    /// 2. Sets the shutdown flag
    pub fn install(redis: Arc<RedisService>) -> Arc<Self> {
        let flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag = flag.clone();

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            tracing::warn!("SIGINT received, shutting down server and worker...");
            // Clear running keys so recovery on next start is immediate
            if let Err(e) = redis.delete_all_running().await {
                tracing::warn!(error = %e, "Failed to clear running keys on shutdown");
            }
            shutdown_flag.store(true, Ordering::Relaxed);
        });

        Arc::new(Self { flag })
    }

    /// Returns `true` if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }

    /// Returns a clone of the shutdown flag for use in async contexts.
    pub fn flag(&self) -> Arc<AtomicBool> {
        self.flag.clone()
    }
}
