#!/usr/bin/env bash
# Check the no-overselling and event-delivery invariants after a load run,
# across all four service databases.
#
# Both event feeds are asynchronous (checkout outbox -> Kafka -> analytics,
# and checkout outbox -> Kafka -> inventory), so first wait for them to
# drain: until the relay has published everything in the outbox, inventory
# has applied up to the end of transaction-completed, and analytics' window
# count stops moving.
set -euo pipefail
cd "$(dirname "$0")"
source ./db-env.sh

# The \copy ... FROM PROGRAM in the SQL runs a child psql; give it the same login.
export PGHOST="$DB_HOST" PGPORT="$DB_PORT" PGUSER="$DB_USER"

end_offset() { # topic
  docker exec selfcheckout-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$1" | awk -F: '{print $3}'
}

echo "Waiting for the event feeds to drain..."
last=-1
for _ in $(seq 1 60); do
  relay_lag="$(psql_db checkout_db -tAc "SELECT COALESCE(max(seq),0) - (SELECT published_seq FROM relay_state) FROM outbox")"
  inventory_lag="$(( $(end_offset transaction-completed) - $(psql_db inventory_db -tAc "SELECT next_offset FROM inventory_state") ))"
  windows="$(psql_db analytics_db -tAc "SELECT count(*) FROM popular_item_windows")"
  if [ "$relay_lag" = "0" ] && [ "$inventory_lag" = "0" ] && [ "$windows" = "$last" ]; then
    break
  fi
  echo "  relay lag $relay_lag, inventory lag $inventory_lag, windows $windows"
  last="$windows"
  sleep 1
done

echo "transaction-completed.DLT holds $(end_offset transaction-completed.DLT) dead letter(s) (expected 0)."
psql_db checkout_db -X -q -f verify-invariants.sql
