#!/usr/bin/env bash
# Create the self_checkout database if it does not already exist.
set -euo pipefail
cd "$(dirname "$0")"
source ./db-env.sh

if psql_super -tAc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1; then
  echo "Database '$DB_NAME' already exists."
else
  psql_super -c "CREATE DATABASE \"$DB_NAME\""
  echo "Created database '$DB_NAME'."
fi
