//! Worker — runs the scheduler loop, dequeuing and executing jobs.
//!
//! Built on top of [`Services`] (the shared Mongo + Redis + Storage +
//! HTTP + Heartbeat + Config handle). One `Worker` per process; spawned
//! by either `serve()` (the gRPC entry point) or by integration tests.

use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::{
    error::ToolError,
    handlers::{
        jobs::{ChunkJobHandler, FileJobHandler},
        scheduler::scheduler_loop,
    },
    services::Services,
    worker::shutdown::Shutdown,
};

/// The worker — runs the scheduler loop, dequeuing and executing jobs.
pub struct Worker {
    services: Arc<Services>,
    shutdown: Arc<Shutdown>,
    worker_id: u32,
}

impl Worker {
    /// Create a new worker.
    pub fn new(services: Arc<Services>, shutdown: Arc<Shutdown>, worker_id: u32) -> Self {
        Self {
            services,
            shutdown,
            worker_id,
        }
    }

    /// Spawn the worker as a background task.
    /// Returns a `JoinHandle` that can be awaited for clean shutdown.
    pub fn spawn(self) -> tokio::task::JoinHandle<Result<(), ToolError>> {
        tokio::spawn(async move {
            tracing::info!("Worker auto-started with gRPC server");
            match self.run().await {
                Ok(()) => {
                    tracing::info!("Worker stopped cleanly");
                    Ok(())
                }
                Err(e) => {
                    tracing::error!(error = %e, "Worker exited with error");
                    Err(e)
                }
            }
        })
    }

    /// Run the worker — the actual worker body.
    async fn run(self) -> Result<(), ToolError> {
        let config = self.services.config.clone();

        // Recover orphaned jobs from previous crashes
        match self
            .services
            .redis
            .recover_orphaned_jobs(config.shard_count)
            .await
        {
            Ok(n) => {
                if n > 0 {
                    tracing::warn!(count = n, "Recovered orphaned jobs at worker startup");
                }
            }
            Err(e) => tracing::warn!(error = %e, "Failed to recover orphaned jobs"),
        }

        match self
            .services
            .redis
            .cleanup_orphaned_chunks(&self.services.db)
            .await
        {
            Ok(n) => {
                if n > 0 {
                    tracing::warn!(count = n, "Cleaned up orphaned chunk tracking keys");
                }
            }
            Err(e) => tracing::warn!(error = %e, "Failed to clean up orphaned chunk keys"),
        }

        // Create temp dir
        let temp_dir = &config.temp_dir;
        tokio::fs::create_dir_all(temp_dir).await?;

        let file_handler = Arc::new(FileJobHandler);
        let chunk_handler = Arc::new(ChunkJobHandler);

        tracing::info!("Starting worker mode");
        tracing::info!(
            file_workers = config.file_workers,
            chunk_workers = config.chunk_workers,
            "Worker pool sizes"
        );

        let file_semaphore = Arc::new(Semaphore::new(config.file_workers));
        let chunk_semaphore = Arc::new(Semaphore::new(config.chunk_workers));

        scheduler_loop(
            file_handler,
            chunk_handler,
            self.services.clone(),
            file_semaphore,
            chunk_semaphore,
            self.shutdown.flag(),
            self.worker_id,
        )
        .await?;

        Ok(())
    }
}
