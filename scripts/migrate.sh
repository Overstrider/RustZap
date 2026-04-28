#!/usr/bin/env sh
set -eu
DATABASE_URL="${DATABASE_URL:-postgres://rustzap:rustzap@localhost:5432/rustzap}"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required for scripts/migrate.sh. For containerized deploy use ./scripts/deploy.sh production." >&2
  exit 1
fi

for file in migrations/*.sql; do
  echo "applying: $file"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$file"
done
