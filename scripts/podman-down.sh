#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

RUSTZAP_CONTAINER="${RUSTZAP_CONTAINER:-rustzap_rustzap_1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-rustzap_postgres_1}"
REDPANDA_CONTAINER="${REDPANDA_CONTAINER:-rustzap_redpanda_1}"
FRONTEND_CONTAINER="${FRONTEND_CONTAINER:-rustzap_dev_tester_1}"

podman rm -f "$FRONTEND_CONTAINER" "$RUSTZAP_CONTAINER" "$POSTGRES_CONTAINER" "$REDPANDA_CONTAINER" >/dev/null 2>&1 || true
