#!/bin/bash
set -euo pipefail

echo "=== media-resources-ingestion integration test runner ==="

cleanup() {
  echo "Cleaning up..."
  docker compose down -v 2>/dev/null || true
}
trap cleanup EXIT

echo "Starting Redis + MongoDB..."
docker compose up -d

echo "Waiting for MongoDB..."
for i in $(seq 1 30); do
  if docker compose exec -T mongodb mongosh --quiet --eval 'db.runCommand({ping:1})' 2>/dev/null; then
    echo "MongoDB ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: MongoDB failed to start" >&2
    exit 3
  fi
  sleep 1
done

echo "Waiting for Redis..."
for i in $(seq 1 15); do
  if docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "Redis ready"
    break
  fi
  if [ "$i" -eq 15 ]; then
    echo "ERROR: Redis failed to start" >&2
    exit 3
  fi
  sleep 1
done

# Export URIs — tests use these or fall back to defaults matching docker-compose
export MONGODB_URI="${MONGODB_URI:-mongodb://root:example@localhost:27017/ingestion?authSource=admin}"
export REDIS_URI="${REDIS_URI:-redis://localhost:6379}"
export RUST_LOG="${RUST_LOG:-ERROR}"

echo ""
echo "=== Running integration tests ==="
cargo test -p ingest-core --test run_integration_test -- --nocapture

echo ""
echo "=== All integration tests passed ==="
