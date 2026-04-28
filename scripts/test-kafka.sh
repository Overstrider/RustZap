#!/usr/bin/env sh
set -eu

KAFKA_TEST_PORT="${KAFKA_TEST_PORT:-19092}"
KAFKA_TEST_POSTGRES_PORT="${KAFKA_TEST_POSTGRES_PORT:-15432}"
KAFKA_TEST_CONTAINER="${KAFKA_TEST_CONTAINER:-rustzap-redpanda-test}"
KAFKA_TEST_POSTGRES_CONTAINER="${KAFKA_TEST_POSTGRES_CONTAINER:-rustzap-postgres-test}"
KAFKA_TEST_IMAGE="${KAFKA_TEST_IMAGE:-docker.redpanda.com/redpandadata/redpanda:latest}"
KAFKA_TEST_POSTGRES_IMAGE="${KAFKA_TEST_POSTGRES_IMAGE:-docker.io/library/postgres:16}"

cleanup() {
  podman rm -f "$KAFKA_TEST_CONTAINER" >/dev/null 2>&1 || true
  podman rm -f "$KAFKA_TEST_POSTGRES_CONTAINER" >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM
cleanup

podman run -d --rm \
  --name "$KAFKA_TEST_CONTAINER" \
  -p "${KAFKA_TEST_PORT}:9092" \
  "$KAFKA_TEST_IMAGE" \
  redpanda start \
    --overprovisioned \
    --smp=1 \
    --memory=768M \
    --reserve-memory=0M \
    --node-id=0 \
    --check=false \
    --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
    --advertise-kafka-addr=PLAINTEXT://127.0.0.1:${KAFKA_TEST_PORT} >/dev/null

podman run -d --rm \
  --name "$KAFKA_TEST_POSTGRES_CONTAINER" \
  -e POSTGRES_DB=rustzap \
  -e POSTGRES_USER=rustzap \
  -e POSTGRES_PASSWORD=rustzap \
  -p "${KAFKA_TEST_POSTGRES_PORT}:5432" \
  "$KAFKA_TEST_POSTGRES_IMAGE" >/dev/null

ready=0
for _ in $(seq 1 45); do
  if podman exec "$KAFKA_TEST_CONTAINER" rpk cluster info --brokers localhost:9092 >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [ "$ready" != "1" ]; then
  echo "Redpanda did not become ready on 127.0.0.1:${KAFKA_TEST_PORT}" >&2
  exit 1
fi

pg_ready=0
for _ in $(seq 1 45); do
  if podman exec "$KAFKA_TEST_POSTGRES_CONTAINER" pg_isready -U rustzap -d rustzap >/dev/null 2>&1; then
    pg_ready=1
    break
  fi
  sleep 1
done

if [ "$pg_ready" != "1" ]; then
  echo "Postgres did not become ready on 127.0.0.1:${KAFKA_TEST_POSTGRES_PORT}" >&2
  exit 1
fi

KAFKA_TEST_BROKERS="127.0.0.1:${KAFKA_TEST_PORT}" \
KAFKA_TEST_DATABASE_URL="postgres://rustzap:rustzap@127.0.0.1:${KAFKA_TEST_POSTGRES_PORT}/rustzap" \
  cargo test --features external-integrations --test kafka_integration -- --ignored --nocapture --test-threads=1
