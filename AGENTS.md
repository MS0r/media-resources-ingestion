# AGENTS.md

Hard-earned context for working on the `media-resources-ingestion` repo. The
README is the architecture reference; this file is the gotchas reference.

## Commands

```bash
docker compose -f compose.dev.yml up -d   # minimal: Redis + MongoDB (root/example)
docker compose up -d                      # full: + Grafana/Loki/Prometheus/Alloy/ingest-server
cargo build -p ingest-cli                 # builds ingest-core + ingest-cli
cargo test -p ingest-core --lib           # unit tests, no external deps
cargo test -p ingest-core                 # FULL suite — self-contained, needs Docker:
                                          # each integration test starts its own
                                          # mongo+redis via testcontainers (see
                                          # tests/common/mod.rs)
./run_test.sh                             # compose.dev down -v + up + cargo run -p ingest-server
./run-integration-tests.sh                # docker compose up + cargo test -p ingest-core --test run_integration_test
cargo fmt && cargo check                  # pre-commit / pre-PR
```

Setting `MONGODB_URI` / `REDIS_URI` env vars makes every test skip
container startup and use those endpoints instead (this is what
`run-integration-tests.sh` relies on). **Warning**: `run-integration-tests.sh`
runs `docker compose down -v` on exit — it tears down the *root* compose
project (same default project name as `compose.dev.yml`) and destroys its
volumes. Don't run it while a dev stack you care about is up.

`./run_test.sh` only starts the gRPC server, it does not run a YAML — open
another terminal and `cargo run -p ingest-cli -- run config-test.yaml`.

**`prove.sh` is broken** — it assigns `cargo run ...` to a shell variable
without executing it, then tries arithmetic on the string. Don't use it;
use `run_test.sh` or `run-integration-tests.sh` instead.

**System deps**:
- `ffmpeg` + `ffprobe` on PATH (runtime, for video compression).
- **Build-time** `ffmpeg-next` bindings: `cmake`, `clang`, `ninja-build`,
  `golang-go`, `libavcodec-dev`, `libavdevice-dev`, `libavfilter-dev`,
  `libavformat-dev`, `libavutil-dev`, `libswresample-dev`, `libswscale-dev`
  (Debian/Ubuntu names). Required to build any crate in the workspace.
- `protoc` on PATH (proto codegen in `ingest-core/build.rs`, or set `PROTOC`).

Rust 2024 edition.

**No CI** — no `.github/workflows/`.

**Docker**: both `compose.dev.yml` and the root `docker-compose.yml` hardcode
`MONGO_INITDB_ROOT_USERNAME: root` / `MONGO_INITDB_ROOT_PASSWORD: example` —
neither one reads `MONGO_USERNAME`/`MONGO_PASSWORD` from `.env`. The `.env`
file at the repo root holds OAuth client IDs/secrets as plain text (not for
production use).

## Cargo workspace

Three crates: `ingest-core`, `ingest-cli`, `ingest-server`. The `ingest-core`
lib is renamed to `ingest_core` (`ingest-core/Cargo.toml:6-8`). Import as
`use ingest_core::...`.

**Re-exports from `ingest-core/src/lib.rs`** (the public surface for
`ingest-cli`/`ingest-server`):

- `enqueue`, `RunConfig`, `TomlRawConfig`
- `JobStatus`, `JobStatusFilter`, `SourceAuth`
- `ToolError`
- `JobEffect`, `JobOutcome`
- `AppConfig`, `OutputFormat`
- `MongoService`, `Provider`
- `Shutdown`, `Worker`

There is **no** `EnqueueConfig`. The gRPC server lives at
`ingest_core::grpc::serve` (not `ingest_core::serve`).

## CLI is a gRPC client

Every CLI subcommand except `server` proxies through a gRPC server at
`[::1]:50051` (`--server` or `INGEST_SERVER_ADDR`). Start it with
`cargo run -p ingest-cli -- server` or `cargo run -p ingest-server`.
The server auto-starts a worker via `Worker::new(...).spawn()`
(`ingest-core/src/grpc/server.rs:528-529`).

`ffmpeg_next::init().ok()` is called in `ingest-cli/src/commands.rs:212`
(`handle_server`) and in `ingest-core/src/grpc/server.rs:505` (`serve()`).
The `ingest-server` binary itself does not call it directly — it goes
through `ingest_core::grpc::serve`.

**Follow mode is the only path that bypasses gRPC.** `ingest run` subscribes
to `jobs:progress:<id>` Redis pubsub channels from the CLI process
(`ingest-cli/src/commands.rs:155-160`) — requires `REDIS_URI` in the CLI's
environment. Use `--no-follow` to skip the Redis dependency on the client.

## Architecture

Two-phase pipeline: **enqueue** (`bootstrap::enqueue` at
`ingest-core/src/bootstrap.rs:44`, parse YAML → Mongo + Redis) → **worker**
(`scheduler_loop` at `ingest-core/src/handlers/scheduler.rs:23`, `BZPOPMAX`
dequeue → download → dedup → compress → store). Both phases share
`AppConfig`.

**Worker startup** (in `ingest-core/src/worker/worker.rs:60-86`) runs
`recover_orphaned_jobs()` then `cleanup_orphaned_chunks()` before joining
the scheduler loop. Construct via `Worker::new(services, shutdown, worker_id)`
and call `.spawn()`.

`Worker::new` consumes an `Arc<Shutdown>` whose `Shutdown::install()`
(`ingest-core/src/worker/shutdown.rs:17`) registers a single SIGINT handler
that calls `redis.delete_all_running()` then flips the shutdown flag.
Don't wire `tokio::signal::ctrl_c()` anywhere else — there should be one
process-wide handler.

**Scheduler dispatch** is in `ingest-core/src/handlers/scheduler.rs`. Job
results come back as one of three `JobOutcome` variants (`job/job_outcome.rs`):

```rust
pub enum JobOutcome {
    Done(JobEffect),                       // success — see effect variant
    Retry { reason: String, backoff_secs: u64 },
    Fail   { reason: String },
}

pub enum JobEffect {
    FileStored     { metadata: Metadata },                   // finished file
    ChunksSpawned  { chunks: Vec<ChunkJob> },                // file → chunks
    ChunkStored    { chunk_ref: ChunkRef, mime: String },    // one chunk done
    DuplicateSkipped,                                        // dedup hit
}
```

**Heartbeat**: every executing job is registered with the
`HeartbeatSupervisor` (`services/heartbeat.rs`), which renews the
`jobs:running:<id>` lease every 10s via a 10s ticker
(`heartbeat.rs:28`). On SIGINT, `delete_all_running()`
(`services/redis.rs:459`) clears every `jobs:running:*` key so the next
worker's `recover_orphaned_jobs` picks them up immediately instead of
waiting for the TTL.

> **Drift**: `.ingest/config.toml` says "heartbeat refreshes every 30s" but
> the code is 10s. The TOML comment is the stale one.

**Shutdown drain**: when the shutdown flag flips, the scheduler stops
dequeuing but waits up to `shutdown_grace_secs` (TOML
`scheduler.shutdown_grace_secs`, default 30s) for the in-flight `JoinSet`
to finish before aborting.

## Sharding

Pending jobs live in Redis sorted sets keyed by `jobs:pending:{N}` where
`N ∈ [0, shard_count)` (default `shard_count = 16`, configured via
`scheduler.shard_count` in the TOML).

**Enqueue** routes each job to one shard:
- **File jobs**: `compute_shard(job_id, shard_count)` (`services/redis.rs:53`)
- **Chunk jobs**: `compute_shard(parent_job_id, shard_count)`

**Dequeue** uses a variadic `BZPOPMAX` across **all** `shard_count` keys in
a single Redis call (`services/redis.rs:230`). The `worker_id` parameter
is no longer used for shard routing — it only appears in the
`jobs:running:<id>` lease tag for stale-lease forensics. `worker_id` is
derived from `INGEST_WORKER_ID` env or `crc32(hostname+pid)`
(`derive_worker_id()`, `services/redis.rs:63`).

**Other shard-aware operations** iterate all shards explicitly:
- `cancel_job` / `cancel_batch_jobs` — pipeline `ZREM` across all shard keys
  (both now take `shard_count: u32` — `redis.rs:484`, `redis.rs:508`).
- `recover_orphaned_jobs` — re-enqueues orphaned jobs to the correct shard
  via `compute_shard`.
- `promote_due_delayed` — encodes the shard in the delayed-queue score and
  promotes back to the correct shard.

**Cluster caveat**: the `{N}` hash tags make each shard key land on a
different Redis Cluster slot. Variadic `BZPOPMAX` across keys on different
slots is not allowed under Cluster mode — single-key-per-shard polling
would be needed.

## Config resolution

**CLI > YAML > TOML**, merged per-field. `dest.path` is required per-resource
after inheritance.

| Field | Sources |
|---|---|
| `priority` | CLI `--priority` > YAML > 0 |
| `file_workers` / `chunk_workers` | TOML `scheduler.*_workers` |
| `compression_override` | YAML only |
| `headers` / `quality` / `source_auth` | YAML > TOML |
| `temp_dir` | TOML only (default `/tmp/ingest`) |
| `chunk_size` | YAML > TOML `storage.chunk_size` |
| retry config | TOML `retry.*`, hardcoded fallback `3600s` / `3` / `[5,30,120]` |

`compression.quality` → `AppConfig.compression_quality`.
`compression.max_compression_seconds` → `AppConfig.compression_timeout_secs`.

## dotenvy scope

`dotenvy::dotenv().ok()` runs only in binary entrypoints:
`ingest-cli/src/main.rs:69` and `ingest-server/src/main.rs:21`. Library code
in `ingest-core` cannot rely on `.env` being loaded — tests must set env
vars explicitly (`tests/run_integration_test.rs` has `mongo_uri()` /
`redis_uri()` helpers that fall back to defaults).

## Auth: two paths

**Storage auth** (`ProviderCache` in `ingest-core/src/storage.rs`): created
once at startup. Silently falls back to a non-functional stub when env vars
are missing — errors surface only as `tracing::error!` when that provider is
actually used. Env vars per provider:

| Provider | Env vars |
|---|---|
| GDrive | `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN`, `GDRIVE_PARENT_FOLDER_ID` (default `root`) |
| Dropbox | `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN` |
| S3 | `AWS_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` (standard AWS SDK chain) |

**Download source auth** (`AuthProviderRegistry`, built by
`Services::build_auth_registry` in `ingest-core/src/services/builder.rs:78`):
uses `OAuthTokenProvider::from_env_or_file()`
(`ingest-core/src/auth/oauth.rs:82`) which checks `~/.ingest/auth.toml`
first, then falls back to env vars. The TOML load returns an empty
`StoredAuthConfig` (no error) when the file is missing.

**OAuth device flows**: `ingest auth dropbox` / `ingest auth gdrive`
(`ingest-cli/src/auth_commands.rs`) save refresh tokens to
`~/.ingest/auth.toml` so server-side jobs can reuse them.

## Error handling

`ingest-core/src/error.rs` — five types:

- **`AuthResolutionError`** — internal OAuth failure (mapping surface).
- **`AppError`** — the modern unified error with exit codes 1–4 and 130.
- **`ToolError`** — legacy infrastructure/config errors; `From<ToolError>
  for AppError` exists for backward compatibility.
- **`JobError`** — job execution (HTTP, I/O, image, ffmpeg, join).
- **`JobErrorOutcome`** — final dispatch: `Retryable(String)` vs
  `Fatal(String)`. `From<AppError> for JobOutcome` lives in `error.rs:322`.

CLI catch-all at `ingest-cli/src/main.rs:110-125` always exits with code 1
even when `ToolError` would suggest another code (SIGINT → 130 is preserved
via the `tokio::select!` at `main.rs:85-108`).

## Logging

- `-v` INFO, `-vv` DEBUG, `-vvv` TRACE. `--quiet` sets the
  `ingest-cli=error` filter via `env!("CARGO_PKG_NAME")` (`main.rs:33-36`).
- `INGEST_VERBOSE=n` env var overrides `-v` flags (`main.rs:26-29`).
- `LOG_FORMAT=json` overrides the CLI `--log-format` flag (`main.rs:21-24`).
- `is_pipe()` (`main.rs:14`) prints a "Detected piped output, auto-switching
  to JSON mode" warning on piped stdout but **does not** actually switch
  formats (the README's claim of auto-switch is wrong).

## Proto

`ingest-core/proto/ingest.proto` codegen via `tonic-build` in
`ingest-core/build.rs`. Server impl: `ingest_core::grpc::serve()`.

RPCs: `Enqueue`, `GetBatchStatus`, `GetJobStatus`, `ListJobs`, `CancelJob`,
`CancelBatch`, `RetryJob`, `ListFiles`, `GetFile`, `DownloadFile`. There is
**no** `DeleteFile` RPC.

TOML config path: `INGEST_CONFIG` env (default `.ingest/config.toml`).

## CLI commands

Documented in README, but two are misleading:

- `ingest files delete <hash>` — exists in clap but the handler just prints
  `"Delete not yet implemented via gRPC"` (`ingest-cli/src/commands.rs:476`).
  Don't promise file deletion.
- `ingest status jobs` — filter values: `pending`, `running`, `completed`,
  `failed`, `retrying`, `cancelled` (matches `JobStatus` in
  `domain/status.rs:14-21`).

## Test structure

Unit tests inline in `src/*.rs` (`#[cfg(test)]`) — access private APIs
without making modules `pub`. Integration tests in `ingest-core/tests/`:

**Infra harness — `tests/common/mod.rs`**: every test binary starts with
`mod common;` and calls `common::infra().await` once per test. It returns
an `Infra { mongo_uri, redis_uri, _mongo, _redis }` where the containers
are *owned by the struct* and removed by testcontainers' RAII `Drop` when
the test ends. Precedence per service: `MONGODB_URI` / `REDIS_URI` env set
→ used as-is, no container; otherwise a fresh `mongo:latest` /
`redis:latest` on a **random host port**. Key constraints:

- **No statics/`OnceLock` for containers** — statics never run `Drop`
  (testcontainers #528/#745) and the `watchdog` feature only catches
  signals, so static-held containers leak on normal exit. One `Infra` per
  test function instead.
- In testcontainers 0.28 the `ImageExt::with_env_var` builder returns
  `ContainerRequest<I>` via a blanket impl — the chain needs an explicit
  type annotation (`let c: ContainerAsync<GenericImage> = ...`) or type
  inference fails (E0282). Inherent methods (`with_exposed_port`,
  `with_wait_for`) must come *before* `with_env_var`.
- Per-test containers mean each test gets its own Mongo + Redis — the
  historical cross-test races on shared collections are gone (distinct
  payloads per test remain good practice).
- A SIGKILL'd test run (e.g. `timeout`) skips `Drop` and leaks the pair —
  `docker rm -f $(docker ps -aq --filter label=org.testcontainers.managed-by)`
  cleans up.

Integration test binaries:

- `chunk_e2e_test.rs` — the chunking + reassembly e2e pair:
  `fake_256mib_file_chunks_and_reassembles` (Range server *claims* a
  256 MiB payload and generates bytes procedurally — zero disk — split
  into 16 × 16 MiB chunks) and `small_temp_file_chunks_and_reassembles`
  (real 5 MiB temp file → 5 × 1 MiB chunks with `threshold_mb = 0`).
  Both run enqueue → worker → finalize → `DownloadFile` RPC and verify
  reassembled bytes against the deterministic generator
  (`(offset % 251) as u8`), **not** against `file_hash` (Merkle root ≠
  content hash).
- `run_integration_test.rs` — full enqueue flow (no worker). Needs
  MongoDB + Redis via `common::infra()`.
- `download_file_test.rs` — boots a real `IngestServer` on an ephemeral
  port, inserts a `Metadata` fixture, drives the `DownloadFile` RPC
  end-to-end.
- `redis_shard_test.rs` — verifies jobs enqueue onto the expected
  `jobs:pending:{N}` shard (`compute_shard`) and that dequeue pops them
  across all shards.
- `chunk_finalize_test.rs` — boots a local HTTP Range server, enqueues a
  file split into 4 chunks, runs the worker, verifies the manifest and
  Merkle-root `file_hash`. Covers the chunk save-first invariant in
  `Hard-earned facts` below. Its TOML sets **`threshold_mb = 0`** — with
  a non-zero threshold the 4 MiB payload never exceeds it and no chunks
  spawn (the chunk decision at `execute.rs:296` is a strict `>`), so do
  not "restore" a realistic threshold here.

`RedisService::flush_db()` exists as a test-only helper.
`run-integration-tests.sh` waits for Mongo+Redis readiness, exports
`MONGODB_URI`/`REDIS_URI` defaults with `?authSource=admin` (bypassing
container startup), and runs `cargo test -p ingest-core --test
run_integration_test -- --nocapture`.

## Reassembly / Download (`DownloadFile` RPC)

Server-streaming RPC returning **original (decompressed) bytes** of any stored
file (`grpc/server.rs:415-499`, `handlers/assemble.rs`). First frame is a
header (mime, filename, size, is_chunked), subsequent frames carry bytes.
Optional inclusive `range_start`/`range_end`.

**Merkle root gotcha**: for chunked files, `Metadata.file_hash` is a SHA-256
of concatenated chunk hashes, NOT a hash of the reassembled file. Cannot
content-verify a reassembly against `file_hash`.

**Known limitations**: range reads decode whole containing chunk (prefix
discarded); sequential chunk fetch; GDrive/Dropbox/S3 `download()` buffer
full object into memory.

**Manifest compression semantics**: `manifest.compression` is `Option<String>`.
`None` means no chunk-level compression was applied (passthrough). `Some("gzip")`
means chunks were gzip-compressed. The per-chunk `ChunkRef.compression` (a
`String`, `""` = passthrough) drives actual decompression in `stream_chunks`
(`handlers/assemble.rs:178`).

## Hard-earned facts

- **`enqueue_chunks` must save to Mongo before pushing to Redis** — the previous
  Redis-first order was a silent chunk-loss race: workers dequeued chunks from
  Redis before the `chunks_jobs` row existed in Mongo, `build_chunk_context`
  returned `None`, and `fail_job` marked the chunk as failed without re-enqueueing.
  Fix: `db.save_chunk_job(chunk.clone())` first, then `redis.enqueue_chunk_job(&chunk)`
  (`handlers/scheduler.rs:575-578`). `ChunkJob` is small (strings + ints +
  Provider enum), so the clone is cheap. If you ever change this loop, keep
  the save-first invariant or the bug returns.
- **`build_chunk_context` retries on `Ok(None)`** (`services/builder.rs:142`,
  5 × 50 ms = 250 ms total) before giving up. Defense-in-depth — the
  producer fix above should make this never fire, but it covers Mongo
  write-visibility lag.
- **On exhausted chunk context-build retries, re-enqueue (don't `fail_job`)** —
  `fail_job` would silently drop the chunk. The bounded re-enqueue uses
  `redis.requeue_chunk_for_retry` (`services/redis.rs:291`), which clears
  the running lease, resets `jobs:state` to `pending`, and `ZADD`s the
  chunk back to the same shard with priority 0. After `max_retries`
  exhausts, the chunk goes to `failed`.
- **Finalization race**: two chunks can simultaneously finalize the parent
  → second `MongoService::complete_job()` returns an error, logged at
  `handlers/scheduler.rs:819` with `tracing::warn!("Chunk finalization insert
  conflict (race), parent already completed")` and finalization continues.
  `complete_job` (`services/mongo.rs:125`) is idempotent via upsert, so
  the race manifests only on transient errors (not specifically E11000).
- **Missing `Content-Length`** → `content_length = 0`, chunk spawning
  skipped, metadata size = 0. Actual byte count overwrites
  `content_length` after download, but chunk decision stays conservative.
- **`compress_video_local`** checks an `Arc<AtomicBool>` cancelled flag
  inside the `decode_av_frames` `spawn_blocking` for `compression_timeout_secs`
  deadline — this is independent of `job_timeout_secs`
  (`handlers/jobs/compression.rs:583, 590-594`).
- **`compress_generic_local`** and **`compress_video_local`** are `async fn`
  wrapping `tokio::task::spawn_blocking` — synchronous IO inside.
  **Image compression** (`compress_image_local`) is also `async` but
  dispatches CPU work through `spawn_blocking` and reads pixels via the
  `image` crate.
- **`GenericCompressionStrategy` deserialization is lowercase**:
  `originalformat`, `gzip`, `zstd`, `zip`, `sevenz`, `none`
  (`compression/generic.rs:10-20`, default is `OriginalFormat`).
- **`ChunkJob` is self-sufficient**: carries its own `url`, auth headers,
  `dest_path` — no parent `FileJob` lookup at download time. Its `storage`
  is inherited from the resource `dest.provider` config (defaults to
  `Provider::Local`).
- **Uses `wreq`, not `reqwest`** — Chrome124 UA emulation.
- **Auth.toml load order**: `load_auth_config()` (`auth/device_auth.rs:235`)
  returns `Default::default()` (empty `StoredAuthConfig`) when
  `~/.ingest/auth.toml` is missing — no error. `OAuthTokenProvider::from_env_or_file()`
  falls back to env vars when the TOML is absent or has no entry for that
  provider.
- **`upsert_file_metadata` placeholder URL/Provider**: the `$setOnInsert`
  placeholders in `services/mongo.rs:181-187` must use valid serde values:
  `original_url` must be a valid URL (currently `"placeholder://pending"`,
  not `""`) and `storage_provider` must be lowercase (`"local"`, not
  `"Local"` — `Provider` uses `rename_all = "lowercase"`). Empty string
  fails `Url::parse`; capital-L fails the serde enum variant lookup.
  `complete_job` overwrites all fields via `$set`.
- **Redis client response timeout must exceed the `BZPOPMAX` block.**
  redis-rs 1.2.2 defaults `response_timeout` to **500 ms**
  (`DEFAULT_RESPONSE_TIMEOUT` in `client.rs`), while `dequeue_job` issues
  `BZPOPMAX ... 2.0` (2 s server-side block). Left at the default, every
  *idle* dequeue fails with `RedisError(timed out)` instead of returning
  `Ok(None)`, and `dequeue_returns_none_when_empty` fails. `get_connection`
  (`services/redis.rs`) therefore builds connections with
  `AsyncConnectionConfig::new().set_response_timeout(Some(3s))`. The
  setter is on `AsyncConnectionConfig`, **not** on `Client` — use
  `get_multiplexed_async_connection_with_config`.
- **Chunked downloads of already-compressed files** — `default_chunk_compression(mime)`
  (`compression/plan.rs:76`) maps already-compressed MIME types (RAR, ZIP,
  7z, MP4, MKV, WebM, MP3, OGG, FLAC, JPEG, PNG, WebP, AVIF, PDF) to
  `GenericCompressionStrategy::OriginalFormat`, which serializes to `""`
  per-chunk and `None` in the manifest. No decompression is applied at
  reassembly time.
- **Every required `Metadata` field must be in `$setOnInsert` too** — the
  struct has non-Option fields (`file_hash`, `original_url`,
  `storage_provider`, `storage_path`, `original_file_size`, `mime_type`,
  `upload_date`, `duplicate_reference_count`). All must appear in the
  `$setOnInsert` block of `upsert_file_metadata` (`services/mongo.rs`)
  so fresh inserts don't crash the BSON deserializer on reads. When adding a
  new non-Option field to `Metadata`, add it to BOTH the struct AND the
  `$setOnInsert` block — or the next `get_file_metadata` / `list_files` read
  will fail with "missing field".
  **Exception — `duplicate_reference_count`**: it must NOT appear in
  `$setOnInsert`, because the same update document `$inc`s it (line ~175);
  listing the path in both operators makes MongoDB reject the whole update
  with error code 40 (`ConflictingUpdateOperators`: "Updating the path
  'duplicate_reference_count' would create a conflict"). The `$inc`
  initializes the field to 1 on insert, so the deserializer still sees it.
  That conflict broke **every single-file download** (job failed at the
  dedup check `execute.rs:409`, retried ×3, then failed for good) until it
  was removed.
- **README's "Current Limitations" / Storage Providers table is stale**.
  All four providers (`Local`, `Gdrive`, `Dropbox`, `S3`) are fully
  implemented — `grep -rn 'todo!\|unimplemented' ingest-core/src/providers`
  returns nothing. The README's "Dropbox stub" / "S3 stub" rows are wrong.
- **Chunk compression strategy reflects the runtime choice, not the request.**
  `compress_generic_local` (`handlers/jobs/compression.rs:800`) returns a
  tuple `(path, size, applied_strategy)` where `applied_strategy` is the
  strategy that was actually used. For the size-compare fallback (gzip grew
  the data), `applied_strategy = OriginalFormat` and the returned path is
  the original input (no `.gz` extension). The chunk handler in
  `handlers/jobs/execute.rs` records this in `ChunkRef.compression`
  (per-chunk). At finalize, the manifest's `compression` field is derived
  from per-chunk `ChunkRef.compression`, NOT from the requested
  `chunk_job.compression_strategy` (`scheduler.rs:776-796`). The
  reassembler reads `chunk_refs[i].compression` per chunk when
  decompressing. **The invariant is: the bytes on disk and
  `ChunkRef.compression` must always agree.** Otherwise reassembly
  produces garbage (raw bytes, but path/manifest claim gzip). If you
  change this loop, keep the recording in sync.
- **Docker builder stage** requires `protoc`, `ffmpeg-dev` libs, `cmake`,
  `clang`, `ninja-build`, `golang-go` (for ffmpeg-next); runtime image has
  only `ffmpeg` + `ca-certificates` (see `Dockerfile`).
- **Observability stack** (Grafana/Loki/Prometheus/Alloy) lives under
  `.ingest/grafana/` and is activated only by the root `docker-compose.yml`.
  **Bug**: `docker-compose.yml:39` mounts `.ingest/grafana/loki-config.yaml`
  (with a dash) but the actual file is `loki_config.yaml` (underscore). The
  Loki container fails to start until the mount path is renamed. The
  `ds.yaml` and `config.alloy` mounts use the correct underscore name.