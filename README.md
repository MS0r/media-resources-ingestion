# ingest

A distributed media ingestion pipeline: download remote resources over HTTP, deduplicate by SHA-256 hash, apply compression (image, video, generic archive), and store to local filesystem — all through a Redis-backed priority queue and gRPC API.

## Architecture

```
┌──────────────┐    gRPC     ┌──────────────────────────────────┐
│  ingest-cli  │ ──────────> │        ingest-server             │
│ (gRPC client)│             │  ┌────────────────────────────┐  │
└──────────────┘             │  │   gRPC API (IngestService) │  │
                             │  └────────────┬───────────────┘  │
                             │  ┌────────────┴───────────────┐  │
                             │  │   Auto-started Worker      │  │
                             │  │   (scheduler_loop)         │  │
                             │  └────────────┬───────────────┘  │
                             └────────────────┼─────────────────┘
                                              │
                        ┌─────────────────────┼─────────────────┐
                        │                     │                 │
            ┌───────────▼────────────┐ ┌──────▼──────┐     ┌────▼────┐
            |          Redis         │ |   MongoDB   │     │  Local  │
            |                        │ |             |     │ Storage │
            │ jobs:pending (zset)    │ | batches     │     │ (disk)  │
            | jobs:state:*   (hash)  │ | files_jobs  │     └─────────┘
            │ jobs:running:* (string)│ | files_meta  │
            │ jobs:progress:* (pub)  │ | chunks_jobs │
            | jobs:chunk_results:*   | └─────────────┘
            └────────────────────────┘
```

All commands proxy through a gRPC server. The server auto-starts a background worker on boot. The CLI never talks to Redis or MongoDB directly.

## Requirements

- Rust 2024 edition toolchain
- Redis + MongoDB (or `docker compose up -d`)
- `ffmpeg` + `ffprobe` on PATH (for video compression)
- `protoc` on PATH (proto compilation)

## Quick Start

```bash
docker compose up -d                    # Redis :6379, MongoDB :27017
cargo build -p ingest-cli               # or `cargo build` for full workspace
ingest server                           # start gRPC server + worker
ingest run config-test.yaml             # enqueue and follow progress
```

## Configuration

Three layers merged per-field (**CLI > YAML > TOML**):

### TOML (`.ingest/config.toml`)

```toml
[scheduler]
file_workers = 5
chunk_workers = 8
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
chunk_size = "128MB"
temp_dir = "/tmp/ingest"

[retry]
running_job_ttl_secs = 3600
max_attempts = 3
backoff_secs = [5, 30, 120]
```

### YAML ingestion request

```yaml
path: ~/downloads
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
      compression_override: webp
  - url: https://example.com/video.mp4
    name: video
    config:
      compression_override: av1
      quality: 25
```

Per-resource configs inherit from parent on a per-field basis. `dest.path` is required per-resource after inheritance. Duplicate URLs are rejected at enqueue time.

### Environment

Set in `.env` at project root (no fallback):

```
MONGODB_URI="mongodb://root:example@localhost:27017/ingestion?authSource=admin"
REDIS_URI="redis://localhost:6379"
RUST_BACKTRACE=full
```

Optional storage auth: `AWS_*`, `GDRIVE_*`, `DROPBOX_*` env vars.

### Logging

`-v` (INFO), `-vv` (DEBUG), `-vvv` (TRACE). `--quiet` sets `ingest-cli=error`.
`LOG_FORMAT=json` for structured output. `INGEST_VERBOSE=n` env overrides `-v`.
Piped stdout auto-switches to JSON format.

## Pipeline

### Phase 1 — Enqueue

1. Parse YAML → `IngestionConfig`, merge with TOML config + CLI args
2. Generate batch UUID
3. For each resource: merge parent values (priority, compression, headers, dest), create `FileJob` in MongoDB, push `"file:<uuid>"` to Redis `jobs:pending` sorted set (score = priority)
4. Save batch document in MongoDB

### Phase 2 — Worker (scheduler_loop)

The `scheduler_loop` runs in a single task, dispatching jobs to a concurrent pool:

1. **Dequeue**: `BZPOPMAX` from `jobs:pending` with 2s timeout — highest priority first (higher score = higher priority)
2. **File job execution**: acquires file semaphore permit → HEAD preflight → if size > threshold (512MB) and server supports Range → split into `ChunkJob`s and enqueue them → otherwise GET full file → stream to temp with SHA-256 hashing → dedup check via MongoDB upsert → compress (if applicable) → upload → save metadata
3. **Chunk job execution**: acquires chunk semaphore permit → Range download → compress (gzip default) → upload → register `ChunkRef` in Redis → on last chunk: build `Manifest`, compute Merkle root hash, finalize parent `FileJob`
4. **Heartbeat**: 10s lease renewal on `jobs:running:<id>` (TTL 3600s)
5. **Retry**: exponential backoff [5s, 30s, 120s], max 3 attempts, score = 0 on retry
6. **Crash recovery**: worker startup scans `jobs:running:*` keys, re-enqueues orphans as pending

### Deduplication

- SHA-256 hash computed during streaming download
- MongoDB `files_metadata` has unique index on `file_hash`
- `find_one_and_update` with upsert: returns `Inserted` or `Duplicate`
- On duplicate: temp file deleted, `duplicate_reference_count` incremented, job completed without new metadata

## Compression

| Category | Strategies | Library | Execution |
|---|---|---|---|
| **Image** | AVIF, WebP, Lossless WebP | `image` crate (async) | Converts JPEG/PNG/GIF; keeps original if compressed is larger |
| **Video** | H.264 (libx264), H.265 (libx265), AV1 (libaom-av1) | `ffmpeg-next` | `spawn_blocking`, CRF from quality, timeout via cancelled flag |
| **Generic** | gzip, zstd, zip, 7z, original format, none | `flate2`, `zstd`, `zip`, `sevenz-rust` | `spawn_blocking`; keeps original if not smaller |

`compression_override` per resource: `avif`, `webp`, `losslesswebp` (image); `h264`, `h265`, `av1` (video); `gzip`, `zstd`, `zip`, `sevenz`, `none`, `originalformat` (generic).

## MIME Detection

Three-tier: HTTP `Content-Type` header → URL extension (`mime_guess`) → bytes magic numbers (`mimetype-detector`, first 3072 bytes, ~550 formats). Tier 3 overrides earlier when unambiguous.

## Data Model

### MongoDB (`ingestion` database)

| Collection | Document | Key fields |
|---|---|---|
| `batches` | `Batch` | `_id`, `created_at`, `yaml_path`, `status`, `job_ids` |
| `files_jobs` | `FileJob` | `_id`, `batch_id`, `resource`, `priority`, `status`, `retry_count`, `file_hash`, `error` |
| `files_metadata` | `Metadata` | `file_hash` (unique index), `original_url`, `storage_provider`, `storage_path`, `original_file_size`, `compressed_file_size`, `compression_ratio`, `mime_type`, `chunk_manifest`, `duplicate_reference_count` |
| `chunks_jobs` | `ChunkJob` | `_id`, `parent_job_id`, `chunk_index`, `offset_start`, `offset_end`, `url`, `auth`, `dest_path`, `total_chunks`, `compression_strategy` |

### Redis keys

| Key pattern | Type | Purpose |
|---|---|---|
| `jobs:pending` | ZSET | Priority queue (score = priority, member = `"file:\|<id>"` / `"chunk:\|<id>"`) |
| `jobs:running:<id>` | String | Worker lease (TTL = 3600s, heartbeat every 10s) |
| `jobs:state:<id>` | Hash | `kind`, `status`, `retry_count`, `error`, `retry_after` |
| `jobs:progress:<id>` | Pub/Sub | Progress events (JSON `ProgressEvent`) |
| `jobs:chunks:<hash>` | Set | Completed chunk hashes (crash recovery) |
| `jobs:chunk_results:<parent_id>` | Hash | `chunk_index` → JSON `ChunkRef` |
| `jobs:counter:<parent_id>` | String | Atomic chunk completion counter |
| `batches:state:<id>` | Hash | Batch status |

## CLI (gRPC client)

```
ingest run <yaml>                  # enqueue + follow progress
ingest run <yaml> --dry-run        # validate + preflight HEAD only
ingest run <yaml> --no-follow      # enqueue only, print Batch ID
ingest server                      # start gRPC server + auto-worker
ingest status batch <id>           # batch status from MongoDB
ingest status job <id>             # single job detail
ingest status jobs [--filter] [--limit]  # list jobs
ingest cancel batch <id>           # cancel all pending jobs in batch
ingest cancel job <id>             # cancel single pending job
ingest retry job <id>              # re-enqueue a failed job
ingest files list [--mime] [--limit]     # list stored file metadata
ingest files get <hash>            # get file metadata by hash
ingest files delete <hash>         # delete file metadata
```

`--server` / `INGEST_SERVER_ADDR` (default `[::1]:50051`) sets gRPC endpoint.

## Chunked Downloads

Files larger than `compression.threshold_mb` (512 MB default) are automatically split into chunk jobs if the server supports HTTP Range requests:

1. HEAD preflight checks `Accept-Ranges: bytes` and `Content-Length`
2. Chunk size from `storage.chunk_size` (e.g. `128MB`)
3. Each `ChunkJob` carries its own URL, auth headers, dest path — no parent lookup at download time
4. Chunks are compressed individually (gzip by default), uploaded, and tracked in Redis
5. On last chunk: all `ChunkRef`s sorted by index → Merkle root (SHA-256 of concatenated chunk hashes) → `Metadata` with `Manifest` → parent `FileJob` finalized
6. Finalization race: two chunks may simultaneously finalize → second gets Mongo duplicate-key error (logged and ignored)

## Storage Providers

| Provider | Status |
|---|---|
| **Local** | Fully implemented — upload, download, health check |
| GDrive | Upload partially implemented |
| Dropbox | Stub (`todo!()`) |
| S3 | Stub (`Err("Not implemented")`) |

## Test commands

```bash
cargo test -p ingest-core --lib              # unit tests (no deps)
./run_test.sh                                # docker compose + cargo run config-test.yaml
./run-integration-tests.sh                   # docker compose + integration tests
cargo fmt && cargo check
```

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Job failure |
| 2 | Config/validation error |
| 3 | Backend connectivity error |
| 4 | Auth error |
| 130 | SIGINT (Ctrl+C) |

## Current Limitations

- Only `LocalProvider` works; GDrive partial, Dropbox/S3 stubs
- Video compression runs synchronously in `spawn_blocking` (ffmpeg bindings)
- FTP URL accepted for preflight, no actual download
- PDF compression not implemented
