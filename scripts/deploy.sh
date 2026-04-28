#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

ENV_NAME="${1:-production}"
ENV_FILE="${RUSTZAP_ENV_FILE:-.env.${ENV_NAME}}"
SECRETS_FILE="${RUSTZAP_SECRETS_FILE:-.env.secrets}"
IMAGE_NAME="${RUSTZAP_IMAGE:-localhost/rustzap_rustzap:latest}"
NETWORK_NAME="${RUSTZAP_NETWORK:-rustzap_default}"
RUSTZAP_CONTAINER="${RUSTZAP_CONTAINER:-rustzap_rustzap_1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-rustzap_postgres_1}"
REDPANDA_CONTAINER="${REDPANDA_CONTAINER:-rustzap_redpanda_1}"
MIGRATE_CONTAINER="${MIGRATE_CONTAINER:-rustzap_migrate}"
FRONTEND_CONTAINER="${FRONTEND_CONTAINER:-rustzap_dev_tester_1}"
FRONTEND_ENV_FILE="${RUSTZAP_FRONTEND_ENV_FILE:-dev-tester/.env.local}"
START_DEV_TESTER="${RUSTZAP_START_DEV_TESTER:-}"

if [ -z "$START_DEV_TESTER" ]; then
  case "$ENV_NAME" in
    dev|development|local) START_DEV_TESTER=1 ;;
    *) START_DEV_TESTER=0 ;;
  esac
fi

if [ ! -f "$ENV_FILE" ]; then
  echo "Missing $ENV_FILE. Create it from .env.${ENV_NAME}.example first." >&2
  exit 1
fi

if [ ! -f "$SECRETS_FILE" ]; then
  echo "Missing $SECRETS_FILE. Create it from .env.secrets.example first." >&2
  exit 1
fi

if [ "$START_DEV_TESTER" = "1" ] && [ ! -f "$FRONTEND_ENV_FILE" ]; then
  echo "Missing $FRONTEND_ENV_FILE. Create it from dev-tester/.env.local.example first." >&2
  exit 1
fi

if ! command -v podman >/dev/null 2>&1; then
  echo "podman is required." >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required for readiness checks." >&2
  exit 1
fi

source_env_file() {
  case "$1" in
    /*|*/*) . "$1" ;;
    *) . "./$1" ;;
  esac
}

detect_lan_host() {
  ip -4 route get 1.1.1.1 2>/dev/null | awk '{
    for (i = 1; i <= NF; i++) {
      if ($i == "src") {
        print $(i + 1)
        exit
      }
    }
  }'
}

set -a
source_env_file "$ENV_FILE"
source_env_file "$SECRETS_FILE"
set +a

: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set in $SECRETS_FILE}"
: "${DATABASE_URL:?DATABASE_URL must be set in $SECRETS_FILE}"

export RUSTZAP_ENV_FILE="$ENV_FILE"
export RUSTZAP_SECRETS_FILE="$SECRETS_FILE"

LAN_HOST="${RUSTZAP_LAN_HOST:-$(detect_lan_host)}"
if [ -z "$LAN_HOST" ]; then
  LAN_HOST="$(hostname -I 2>/dev/null | awk '{print $1}')"
fi
if [ -z "$LAN_HOST" ]; then
  LAN_HOST="127.0.0.1"
fi

mkdir -p \
  data/rustzap \
  data/wa-sessions \
  data/logs \
  data/postgres \
  data/redpanda

chmod -R a+rwX data/rustzap data/wa-sessions data/logs data/postgres data/redpanda

if ! podman network exists "$NETWORK_NAME" >/dev/null 2>&1; then
  podman network create "$NETWORK_NAME" >/dev/null
fi

remove_container() {
  podman rm -f "$1" >/dev/null 2>&1 || true
}

echo "Building RustZap image..."
podman build -t "$IMAGE_NAME" -f Containerfile .

echo "Starting Postgres and Redpanda..."
remove_container "$FRONTEND_CONTAINER"
remove_container "$RUSTZAP_CONTAINER"
remove_container "$POSTGRES_CONTAINER"
remove_container "$REDPANDA_CONTAINER"

podman run -d \
  --name "$POSTGRES_CONTAINER" \
  --network "$NETWORK_NAME" \
  --network-alias postgres \
  -e POSTGRES_DB=rustzap \
  -e POSTGRES_USER=rustzap \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -v "$ROOT_DIR/data/postgres:/var/lib/postgresql/data:Z" \
  docker.io/library/postgres:16 >/dev/null

podman run -d \
  --name "$REDPANDA_CONTAINER" \
  --network "$NETWORK_NAME" \
  --network-alias redpanda \
  -v "$ROOT_DIR/data/redpanda:/var/lib/redpanda/data:Z" \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start \
  --overprovisioned \
  --smp=1 \
  --memory=1G \
  --reserve-memory=0M \
  --node-id=0 \
  --check=false \
  --kafka-addr=PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr=PLAINTEXT://redpanda:9092 >/dev/null

echo "Waiting for Postgres..."
attempt=0
until podman exec "$POSTGRES_CONTAINER" pg_isready -U rustzap -d rustzap >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -gt 60 ]; then
    echo "Postgres did not become ready." >&2
    podman logs "$POSTGRES_CONTAINER" >&2 || true
    exit 1
  fi
  sleep 2
done

echo "Applying migrations..."
remove_container "$MIGRATE_CONTAINER"
podman run --rm \
  --name "$MIGRATE_CONTAINER" \
  --network "$NETWORK_NAME" \
  --env-file "$ENV_FILE" \
  --env-file "$SECRETS_FILE" \
  -v "$ROOT_DIR/data/rustzap:/data/rustzap:Z" \
  -v "$ROOT_DIR/data/wa-sessions:/data/rustzap/wa-sessions:Z" \
  -v "$ROOT_DIR/data/logs:/var/log/rustzap:Z" \
  "$IMAGE_NAME" \
  rustzap migrate

echo "Starting RustZap..."
HOST_PORT="${RUSTZAP_HOST_PORT:-8167}"
podman run -d \
  --name "$RUSTZAP_CONTAINER" \
  --network "$NETWORK_NAME" \
  --network-alias rustzap \
  --env-file "$ENV_FILE" \
  --env-file "$SECRETS_FILE" \
  -e "RUSTZAP_PUBLIC_BASE_URL=http://${LAN_HOST}:${HOST_PORT}" \
  -p "${HOST_PORT}:8167" \
  -v "$ROOT_DIR/data/rustzap:/data/rustzap:Z" \
  -v "$ROOT_DIR/data/wa-sessions:/data/rustzap/wa-sessions:Z" \
  -v "$ROOT_DIR/data/logs:/var/log/rustzap:Z" \
  "$IMAGE_NAME" >/dev/null

echo "Waiting for RustZap readiness on http://${LAN_HOST}:${HOST_PORT}/ready..."
attempt=0
until curl -fsS "http://127.0.0.1:${HOST_PORT}/ready" 2>/dev/null | grep -q '"ok":true'; do
  attempt=$((attempt + 1))
  if [ "$attempt" -gt 60 ]; then
    echo "RustZap did not become ready." >&2
    podman logs "$RUSTZAP_CONTAINER" >&2 || true
    exit 1
  fi
  sleep 2
done

if [ "$START_DEV_TESTER" = "1" ]; then
  FRONTEND_PORT="${DEV_TESTER_HOST_PORT:-3167}"
  HOST_UID="$(stat -c '%u' "$ROOT_DIR")"
  HOST_GID="$(stat -c '%g' "$ROOT_DIR")"
  echo "Starting dev tester frontend..."
  podman run -d \
    --name "$FRONTEND_CONTAINER" \
    --network "$NETWORK_NAME" \
    --network-alias dev-tester \
    --user "$HOST_UID:$HOST_GID" \
    -e HOME=/tmp \
    --env-file "$FRONTEND_ENV_FILE" \
    -p "${FRONTEND_PORT}:3167" \
    -w /app \
    -v "$ROOT_DIR/dev-tester:/app:Z" \
    docker.io/library/node:22-bookworm \
    sh -lc 'test -d node_modules || npm install; npm run dev:lan' >/dev/null

  echo "Waiting for dev tester on http://${LAN_HOST}:${FRONTEND_PORT}..."
  attempt=0
  until curl -fsS "http://127.0.0.1:${FRONTEND_PORT}" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -gt 60 ]; then
      echo "Dev tester did not become ready." >&2
      podman logs "$FRONTEND_CONTAINER" >&2 || true
      exit 1
    fi
    sleep 2
  done
fi

podman ps --filter "name=rustzap_" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo "RustZap ready: http://${LAN_HOST}:${HOST_PORT}"
if [ "$START_DEV_TESTER" = "1" ]; then
  echo "Dev tester ready: http://${LAN_HOST}:${FRONTEND_PORT}"
else
  echo "Dev tester skipped. Set RUSTZAP_START_DEV_TESTER=1 to start it."
fi
