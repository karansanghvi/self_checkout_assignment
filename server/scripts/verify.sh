#!/usr/bin/env bash
# Check the no-overselling invariants after a load run.
set -euo pipefail
cd "$(dirname "$0")"
source ./db-env.sh

psql_app -v ON_ERROR_STOP=1 -f verify-invariants.sql
