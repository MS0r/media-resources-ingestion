# media-resources-ingestion

A Rust CLI for ingesting remote media resources into local storage, with deduplication, job queuing, and metadata tracking.

## What It Does

- Reads an ingestion batch from a YAML file
- Creates a batch document and one file job per resource in MongoDB
- Queues jobs in Redis using priority scores (higher value = higher priority)
- Downloads each resource over HTTP
- Streams downloads to local storage while computing a SHA-256 content hash
- Detects duplicate content by hash — skips storing if a match already exists in MongoDB
- Tracks batch, job, and file metadata throughout the lifecycle

## Requirements

- Rust 2024 toolchain
- Redis
- MongoDB
- Docker Compose (optional, for running Redis and MongoDB locally)

## Local Services

Start Redis and MongoDB with:

```bash
docker compose up -d
```

The included `docker-compose.yml` exposes:

- Redis on `localhost:6379`
- MongoDB on `localhost:27017`

## Environment Variables

Create a `.env` file at the project root:

```bash
MONGODB_URI="mongodb://root:example@localhost:27017/ingestion?authSource=admin"
REDIS_URI="redis://localhost:6379"
RUST_LOG=DEBUG
```

If these variables are not set, the CLI falls back to:

- `REDIS_URI=redis://localhost:6379`
- `MONGODB_URI=mongodb://localhost:27017/ingestion`

## Configuration

The CLI expects a TOML configuration file at `.ingest/config.toml` unless overridden with `--config`.

```toml
[cli]
log_format = "Pretty"
no_color = false

[scheduler]
file_workers = 5
chunk_workers = 20
max_pending_jobs = 10000
max_per_host = 2

[compression]
threshold_mb = 512
quality = 95

[storage]
default_provider = "local"
default_path = "~/downloads"

[retry]
attempt_1_secs = 5
attempt_2_secs = 30
attempt_3_secs = 120
```

## Ingestion YAML

Each resource requires a URL and a local destination path. The `dest.path` field is required for the current local storage implementation.

```yaml
provider: local
path: ~/images
priority: 0
chunk_size: 128MB
compression_override: webp
headers:
  Authorization: Bearer Token
  Cookie: session=abc
resources:
  - url: https://example.com/image.webp
    name: image
    priority: 10
    dest:
      provider: local
      path: ~/downloads
    config:
      force_compress: true
      compression_override: webp
      quality: 95
```

### YAML Fields

| Field | Required | Description |
|---|---|---|
| `provider` | Yes | Storage backend: `local` \| `gdrive` \| `dropbox` \| `s3` |
| `path` | Yes | Default destination path |
| `priority` | No | Batch-level scheduling priority (integer, default `0`) |
| `chunk_size` | No | Chunk size for large files (default: `128MB`) |
| `headers` | No | HTTP headers applied to all resource requests |
| `compression_override` | No | Compression format override for all resources |
| `resources[].url` | Yes | Source URL (`http` or `https`) |
| `resources[].name` | No | Output filename; derived from URL if omitted |
| `resources[].priority` | No | Overrides batch-level priority for this resource |
| `resources[].dest.provider` | No | Storage provider for this resource |
| `resources[].dest.path` | No | Destination path for this resource |
| `resources[].config.force_compress` | No | Force compression regardless of file size |
| `resources[].config.compression_override` | No | Compression format for this resource |
| `resources[].config.quality` | No | Lossy quality 0–100 (default `95`) |

## Usage

Build:

```bash
cargo build
```

Run an ingestion batch:

```bash
cargo run -- run config-test.yaml
```

Use a custom config file:

```bash
cargo run -- --config path/to/config.toml run path/to/resources.yaml
```

Show help:

```bash
cargo run -- --help
cargo run -- run --help
```

## Runtime Flow

1. CLI loads `.env`, initialises tracing, and parses arguments
2. `run` loads the TOML config and YAML ingestion request
3. Redis and MongoDB clients are initialised
4. A batch is created with a generated UUID
5. Each YAML resource becomes a pending file job
6. File jobs are written to MongoDB and enqueued in Redis
7. The scheduler dequeues the highest-priority pending job
8. A file worker downloads the resource, streams it to local storage, hashes the content, and records metadata
9. If the file hash already exists in MongoDB, the duplicate is deleted and the counter is incremented

## Data Model

MongoDB uses the `ingestion` database with four collections:

| Collection | Purpose |
|---|---|
| `batches` | One document per ingestion run |
| `files_jobs` | One document per resource, tracks job lifecycle |
| `files_metadata` | File hash, storage path, size, MIME type, and deduplication counter |
| `chunks_jobs` | Reserved for chunk-level jobs (not yet active) |

Indexes on `files_metadata`:

- `file_hash` — unique
- `storage_path`
- `storage_provider`

Redis key schema:

| Key | Type | Purpose |
|---|---|---|
| `jobs:pending` | Sorted Set | Priority queue; `ZPOPMAX` dequeues highest priority first |
| `jobs:running` | Hash | Maps job ID → worker ID for active jobs |
| `jobs:state:<job_id>` | Hash | Full job state: status, retry count, timestamps |
| `batches:state:<batch_id>` | Hash | Batch-level state |

## Job Lifecycle

```
Pending → Running → Completed
                 ↘ Retrying → Running
                 ↘ Failed
```

Retry policy:

| Attempt | Backoff |
|---|---|
| 1st retry | 5 seconds |
| 2nd retry | 30 seconds |
| 3rd retry (final) | 2 minutes |

## CLI Subcommands

Only the `run` command is fully implemented. The remaining subcommands are declared and parse correctly but return "not implemented".

| Subcommand | Status |
|---|---|
| `run <YAML>` | ✅ Implemented |
| `status batch/job/jobs` | 🚧 Not implemented |
| `cancel batch/job` | 🚧 Not implemented |
| `retry job` | 🚧 Not implemented |
| `files list/get/download/delete` | 🚧 Not implemented |

## Current Limitations

- Only `local` storage is active; `gdrive`, `dropbox`, and `s3` are modelled but not wired
- Chunk job execution is not implemented; large files are downloaded as a single stream
- Compression settings and per-resource overrides are parsed and stored but not applied
- FTP/FTPS protocol support is declared in the stack but not connected to the download path
- `status`, `cancel`, `retry`, and `files` command handlers return "not implemented"

## Development

```bash
cargo fmt
cargo check
cargo test   # no tests yet
```