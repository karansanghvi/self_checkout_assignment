#!/usr/bin/env bash
# Wipe all run state and reseed 2,000 items x 10,000 units.
# Run this between load-test runs.
#
#   ./reset-db.sh                      # defaults: 2000 items, 10000 units
#   ./reset-db.sh 500 200              # 500 items, 200 units each
set -euo pipefail
cd "$(dirname "$0")"
source ./db-env.sh

CATALOG_SIZE="${1:-2000}"
STOCK_PER_ITEM="${2:-10000}"

echo "Resetting '$DB_NAME': ${CATALOG_SIZE} items x ${STOCK_PER_ITEM} units..."
psql_app -q -v ON_ERROR_STOP=1 \
         -v catalog_size="$CATALOG_SIZE" \
         -v stock_per_item="$STOCK_PER_ITEM" \
         -f reset.sql
echo "Reset complete."
