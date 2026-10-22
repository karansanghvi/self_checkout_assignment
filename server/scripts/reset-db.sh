#!/usr/bin/env bash
# Reset the whole system between load-test runs: every service wipes its own
# run state and reseeds, under a new epoch, orchestrated by admin-service.
#
# There is no SQL shortcut any more: the data is spread over four databases
# owned by four services, and some of them hold in-memory state (catalog
# replicas, the popularity window, the outbox relay's position) that a
# script editing their tables would leave stale.
#
#   ./reset-db.sh                      # defaults: 2000 items, 10000 units
#   ./reset-db.sh 500 200              # 500 items, 200 units each
set -euo pipefail

CATALOG_SIZE="${1:-2000}"
STOCK_PER_ITEM="${2:-10000}"
ADMIN_URL="${ADMIN_URL:-http://localhost:8085}"

echo "Resetting: ${CATALOG_SIZE} items x ${STOCK_PER_ITEM} units..."
curl -fsS -X POST "$ADMIN_URL/admin/reset?catalogSize=$CATALOG_SIZE&stockPerItem=$STOCK_PER_ITEM"
echo
echo "Reset complete."
