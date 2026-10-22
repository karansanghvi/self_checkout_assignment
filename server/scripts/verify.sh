#!/usr/bin/env bash
# Check the no-overselling and saga invariants after a load run, across all
# four service databases.
#
# The scan feed is asynchronous (outbox -> Kafka -> analytics), so first wait
# for it to drain: until the relay has published everything in the outbox and
# analytics' window count stops moving.
set -euo pipefail
cd "$(dirname "$0")"
source ./db-env.sh

# The \copy ... FROM PROGRAM in the SQL runs a child psql; give it the same login.
export PGHOST="$DB_HOST" PGPORT="$DB_PORT" PGUSER="$DB_USER"

echo "Waiting for the scan feed to drain..."
last=-1
for _ in $(seq 1 30); do
  lag="$(psql_db checkout_db -tAc "SELECT COALESCE(max(seq),0) - (SELECT published_seq FROM relay_state) FROM scan_events")"
  windows="$(psql_db analytics_db -tAc "SELECT count(*) FROM popular_item_windows")"
  if [ "$lag" = "0" ] && [ "$windows" = "$last" ]; then
    break
  fi
  last="$windows"
  sleep 1
done

psql_db checkout_db -X -q -f verify-invariants.sql
