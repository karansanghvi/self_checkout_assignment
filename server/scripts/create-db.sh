#!/usr/bin/env bash
# Create one database and one login role per service.
#
#   catalog_db    <- catalog_svc
#   inventory_db  <- inventory_svc
#   checkout_db   <- checkout_svc
#   analytics_db  <- analytics_svc
#
# Each role owns its database, so its service can run its own migrations,
# and CONNECT is revoked from PUBLIC, so no role can open another service's
# database. (admin-service has no database at all.) Postgres cannot join
# across databases, so this is a hard boundary, not a naming convention.
#
# Idempotent: safe to re-run. Uses the superuser from db-env.sh; the service
# roles' password is SERVICE_DB_PASSWORD from server/.env.
set -euo pipefail
cd "$(dirname "$0")"
source ./db-env.sh
source ./services.sh

for db in "${SERVICE_DATABASES[@]}"; do
  read -r name role <<<"$db"

  if psql_super -tAc "SELECT 1 FROM pg_roles WHERE rolname = '$role'" | grep -q 1; then
    psql_super -q -c "ALTER ROLE \"$role\" WITH LOGIN PASSWORD '$SERVICE_DB_PASSWORD'"
  else
    psql_super -q -c "CREATE ROLE \"$role\" WITH LOGIN PASSWORD '$SERVICE_DB_PASSWORD'"
    echo "Created role '$role'."
  fi

  if psql_super -tAc "SELECT 1 FROM pg_database WHERE datname = '$name'" | grep -q 1; then
    echo "Database '$name' already exists."
  else
    psql_super -q -c "CREATE DATABASE \"$name\" OWNER \"$role\""
    echo "Created database '$name' owned by '$role'."
  fi

  psql_super -q -c "REVOKE CONNECT ON DATABASE \"$name\" FROM PUBLIC"
  psql_super -q -c "GRANT CONNECT ON DATABASE \"$name\" TO \"$role\""
done
