use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::services::redis::RedisService;

struct Entry {
    redis: Arc<RedisService>,
    done: Arc<AtomicBool>,
}

enum Msg {
    Register { job_id: String, entry: Entry },
}

pub struct HeartbeatSupervisor {
    tx: mpsc::UnboundedSender<Msg>,
}

impl HeartbeatSupervisor {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let handle = tokio::spawn(async move {
            let mut active: HashMap<String, Entry> = HashMap::new();
            let mut ticker = tokio::time::interval(Duration::from_secs(10));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                tokio::select! {
                    msg = rx.recv() => match msg {
                        Some(Msg::Register { job_id, entry }) => {
                            active.insert(job_id, entry);
                        }
                        None => {
                            tracing::warn!("HeartbeatSupervisor channel closed, exiting");
                            break;
                        }
                    },
                    _ = ticker.tick() => {
                        let mut done_keys = Vec::new();
                        for (job_id, entry) in &active {
                            if entry.done.load(Ordering::Relaxed) {
                                done_keys.push(job_id.clone());
                                continue;
                            }
                            let _ = entry.redis.renew_lease(job_id).await;
                        }
                        for k in done_keys {
                            active.remove(&k);
                        }
                    }
                }
            }
        });

        tokio::spawn(async move {
            match handle.await {
                Ok(()) => {
                    tracing::warn!("HeartbeatSupervisor task ended (channel closed)")
                }
                Err(e) if e.is_panic() => {
                    tracing::error!(
                        error = ?e,
                        "HeartbeatSupervisor task PANICKED — leases will not be renewed; \
                         recovery depends on jobs:running TTL expiry or process restart"
                    );
                }
                Err(e) => {
                    tracing::error!(error = ?e, "HeartbeatSupervisor task cancelled");
                }
            }
        });

        Self { tx }
    }

    pub fn register(&self, job_id: String, redis: Arc<RedisService>, done: Arc<AtomicBool>) {
        let _ = self.tx.send(Msg::Register {
            job_id,
            entry: Entry { redis, done },
        });
    }
}
