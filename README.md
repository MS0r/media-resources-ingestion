# ingest

A Rust CLI for ingesting remote media resources into local storage, with deduplication, compression, job queuing, and metadata tracking.

## What It Does

- Reads an ingestion batch from a YAML file
- Creates a batch document and one file job per resource in MongoDB
- Queues jobs in Redis using priority scores (higher = higher priority)
- Downloads each resource over HTTP with Chrome UA emulation
- Streams downloads to local storage while computing a SHA-256 hash
- Detects duplicate content by hash — skips storing if match exists
- Applies image (WebP/AVIF) or video (H264/H265/AV1) compression
- Tracks batch, job, and file metadata throughout the lifecycle
- Supports `files list|get|download|delete`, `status`, `cancel`, `retry` commands

## Requirements

- Rust 2024 edition toolchain
- Redis
- MongoDB
- Docker Compose (optional, for running Redis and MongoDB locally)

## Local Services

```bash
docker compose up -d    # Redis :6379, MongoDB :27017
```

## Environment Variables

**Required** — no fallback. Set in `.env` at project root:

```
MONGODB_URI="mongodb://root:example@localhost:27017/ingestion?authSource=admin"
REDIS_URI="redis://localhost:6379"
RUST_BACKTRACE=full
```

Optional auth (set any env var to opt in):
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_S3_BUCKET`, `AWS_REGION`
`GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN`
`DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`

## Configuration

Default TOML path: `.ingest/config.toml` (overridable via `--config`).

```toml
[scheduler]
file_workers = 5
chunk_workers = 20
max_pending_jobs = 10_000
max_per_host = 2
job_timeout_secs = 7200

[compression]
threshold_mb = 512
quality = 95
max_compression_seconds = 300

[storage]
default_provider = "local"
default_path = "~/downloads"
chunk_size = "16MB"
temp_dir = "/tmp/ingest"

[retry]
running_job_ttl_secs = 3600
max_attempts = 3
backoff_secs = [5, 30, 120]
```

Logging: `-v` (INFO), `-vv` (DEBUG), `-vvv` (TRACE). `LOG_FORMAT=json` overrides CLI format. `INGEST_VERBOSE=n` env var overrides `-v` flags. `RUST_LOG` works normally via `tracing-subscriber`.

## Ingestion YAML

```yaml
path: ~/downloads        # default dest path (flattened into default_dest)
priority: 0
chunk_size: 128MB
compression_override: avif
headers:
  Authorization: Bearer Token
resources:
  - url: https://example.com/image.png
    name: perro
    priority: 10
    config:
      quality: 90
  - url: https://example.com/video.mp4
    name: video
    config:
      compression_override: av1
      quality: 25
```

Top-level `provider`/`path` are `#[serde(flatten)]` into `IngestionConfig.default_dest`. Each resource inherits parent values (dest, compression, headers, priority) on a per-field basis — partial resource configs merge rather than being replaced.

`dest.path` is **required per-resource** (after inheritance) — job fails with `"Missing destination path"` if absent.

Duplicate URLs in YAML are rejected at bootstrap (exit code 2).

## Usage

```bash
cargo build
cargo run -- run config-test.yaml          # enqueue + execute
cargo run -- enqueue config-test.yaml      # enqueue only, print Batch ID
cargo run -- worker                        # standalone worker
cargo run -- run config-test.yaml --dry-run # validate + preflight HEAD
cargo run -- status batch <id>
cargo run -- cancel batch <id>
cargo run -- files list
cargo run -- files get <hash>
cargo run -- files download <hash> <dest>
cargo run -- --config path/to/config.toml run path/to/resources.yaml
cargo run -- --help
```

## CLI Commands

| Command | Behavior |
|---|---|
| `ingest run <yaml>` | Phase 1 (enqueue) + Phase 2 (follow mode) |
| `ingest run <yaml> --no-follow` | Phase 1 only, print Batch ID |
| `ingest enqueue <yaml>` | Same as `run --no-follow` |
| `ingest worker` | Phase 2 only, standalone worker (Ctrl+C to stop) |
| `ingest run <yaml> --dry-run` | Validate YAML + preflight HEAD, no download |
| `ingest status batch\|job\|jobs` | Inspect batches and jobs |
| `ingest cancel batch\|job` | Cancel pending jobs |
| `ingest retry job <id>` | Retry a failed job |
| `ingest files list\|get\|download\|delete` | Browse and manage stored files |

`--follow` / `--no-follow` are mutual CLI overrides. Piped stdout auto-disables follow.

## Architecture

Two-phase separation via Redis:

- **Phase 1 (enqueue)**: Parse YAML, validate, create batch + FileJobs in MongoDB, push to Redis `jobs:pending` sorted set
- **Phase 2 (worker)**: `scheduler_loop` — `BZPOPMAX` from Redis, acquire semaphore, download, hash, dedup, compress, store

`ingest run <yaml>` calls both phases sequentially. Multiple workers can dequeue independently. `jobs:running:<id>` TTL handles crash detection.

## Data Model

MongoDB database `ingestion`:

| Collection | Purpose |
|---|---|
| `batches` | One document per ingestion run |
| `files_jobs` | One document per resource, tracks job lifecycle |
| `files_metadata` | File hash, storage path, size, MIME type, dedup counter |
| `chunks_jobs` | Reserved for chunk-level jobs |

### Redis keys

```
jobs:pending         sorted set  (score=priority, member="file:<id>"|"chunk:<id>")
jobs:running:<id>    string      worker_id (per-job key with TTL)
jobs:state:<id>      hash        kind, status, retry_count, error
jobs:chunks:<hash>   set         completed chunk hashes (crash recovery)
batches:state:<id>   hash        status
```

## Runtime Flow

1. CLI loads `.env`, initialises tracing, parses args
2. `run` loads TOML config and YAML ingestion request
3. Redis and MongoDB clients are initialised
4. Batch is created with a generated UUID
5. Each YAML resource becomes a pending file job
6. File jobs written to MongoDB and enqueued in Redis
7. Scheduler dequeues highest-priority pending job
8. Worker downloads resource, streams to temp, SHA-256 hashes, detects MIME by bytes
9. If file hash exists in MongoDB → duplicate, temp file deleted, counter incremented
10. If new → image/video compression (if configured), upload to storage, metadata recorded

## MIME Detection

Three-tier: HTTP `Content-Type` header → URL extension via `mime_guess` → bytes-based via `mimetype_detector` (first 3072 bytes, ~550 formats via magic numbers). Tier 3 overrides earlier detections when unambiguous.

## Current Limitations

- Only `LocalProvider` fully works; GDrive upload partially implemented, Dropbox/S3 stubs
- Chunk job execution is not wired (range download/upload)
- Image (WebP/AVIF) and video (H264/H265/AV1) compression work; PDF/zstd not implemented
- FTP URL scheme accepted for preflight, no actual download

## Tests

```bash
cargo test --lib                        # unit tests only (no external deps)
cargo test                              # all tests (~100, integration fails without services)
./run-integration-tests.sh              # docker compose up → wait → run integration tests
cargo test --test run_integration -- --nocapture  # manual, requires MongoDB + Redis
```

## Development

```bash
cargo fmt && cargo check
```
