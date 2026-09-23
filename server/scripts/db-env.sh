#!/usr/bin/env bash
# Shared DB connection settings, sourced by the other scripts.
#
# DB_PASSWORD is read from, in order of precedence:
#   1. the environment, if already exported
#   2. server/.env  (gitignored; see .env.example)
# There is deliberately no default.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/../.env}"

if [ -z "${DB_PASSWORD:-}" ] && [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

export DB_HOST="${DB_HOST:-localhost}"
export DB_PORT="${DB_PORT:-5432}"
export DB_NAME="${DB_NAME:-self_checkout}"
export DB_USER="${DB_USER:-postgres}"

if [ -z "${DB_PASSWORD:-}" ]; then
  echo "ERROR: DB_PASSWORD is not set, and no .env file was found at:" >&2
  echo "       $ENV_FILE" >&2
  echo >&2
  echo "Fix with one of:" >&2
  echo "  cp server/.env.example server/.env && \$EDITOR server/.env" >&2
  echo "  export DB_PASSWORD='...'   (this shell only)" >&2
  exit 1
fi

# psql reads PGPASSWORD. Never echo it.
export PGPASSWORD="$DB_PASSWORD"

psql_super() { psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres "$@"; }
psql_app()   { psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" "$@"; }
