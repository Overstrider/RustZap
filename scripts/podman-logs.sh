#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

case "${1:-backend}" in
  backend|rustzap)
    CONTAINER="${RUSTZAP_CONTAINER:-rustzap_rustzap_1}"
    ;;
  frontend|dev-tester)
    CONTAINER="${FRONTEND_CONTAINER:-rustzap_dev_tester_1}"
    ;;
  postgres)
    CONTAINER="${POSTGRES_CONTAINER:-rustzap_postgres_1}"
    ;;
  redpanda)
    CONTAINER="${REDPANDA_CONTAINER:-rustzap_redpanda_1}"
    ;;
  *)
    echo "Usage: $0 [backend|frontend|postgres|redpanda]" >&2
    exit 1
    ;;
esac

exec podman logs -f "$CONTAINER"
