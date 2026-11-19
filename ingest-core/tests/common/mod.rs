//! Shared test infrastructure: per-test MongoDB + Redis containers via
//! testcontainers, with env-var override for compatibility with
//! `run-integration-tests.sh`.
//!
//! One [`Infra`] is created per test function. The containers are owned
//! by the returned struct and removed by testcontainers' RAII `Drop`
//! when the test ends (or unwinds). Deliberately no statics/`OnceLock`:
//! statics never run `Drop`, so containers would leak on normal exit.

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Handle to the per-test MongoDB + Redis instances.
///
/// `#[allow(dead_code)]`: each test binary uses a different subset
/// (e.g. `redis_shard_test` never reads `mongo_uri`).
#[allow(dead_code)]
pub struct Infra {
    pub mongo_uri: String,
    pub redis_uri: String,
    _mongo: Option<ContainerAsync<GenericImage>>,
    _redis: Option<ContainerAsync<GenericImage>>,
}

/// Start (or reuse via env) MongoDB + Redis for a single test.
///
/// Precedence per service:
/// - `MONGODB_URI` / `REDIS_URI` env set → used as-is, no container.
/// - otherwise a fresh container on a random host port.
pub async fn infra() -> Infra {
    let (mongo_uri, _mongo) = match std::env::var("MONGODB_URI") {
        Ok(uri) => (uri, None),
        Err(_) => start_mongo().await,
    };
    let (redis_uri, _redis) = match std::env::var("REDIS_URI") {
        Ok(uri) => (uri, None),
        Err(_) => start_redis().await,
    };
    Infra {
        mongo_uri,
        redis_uri,
        _mongo,
        _redis,
    }
}

async fn start_mongo() -> (String, Option<ContainerAsync<GenericImage>>) {
    let container: ContainerAsync<GenericImage> = GenericImage::new("mongo", "latest")
        .with_exposed_port(27017.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
        .with_env_var("MONGO_INITDB_ROOT_USERNAME", "root")
        .with_env_var("MONGO_INITDB_ROOT_PASSWORD", "example")
        .start()
        .await
        .expect("start mongo container");
    let host = container.get_host().await.expect("mongo host");
    let port = container
        .get_host_port_ipv4(27017.tcp())
        .await
        .expect("mongo host port");
    let uri = format!("mongodb://root:example@{host}:{port}/ingestion?authSource=admin");
    (uri, Some(container))
}

async fn start_redis() -> (String, Option<ContainerAsync<GenericImage>>) {
    let container: ContainerAsync<GenericImage> = GenericImage::new("redis", "latest")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .expect("start redis container");
    let host = container.get_host().await.expect("redis host");
    let port = container
        .get_host_port_ipv4(6379.tcp())
        .await
        .expect("redis host port");
    let uri = format!("redis://{host}:{port}");
    (uri, Some(container))
}
