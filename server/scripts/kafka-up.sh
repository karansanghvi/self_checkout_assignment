#!/usr/bin/env bash
# Start the Kafka broker (Docker) and create every topic the services use.
# The broker does not auto-create topics, so a misspelt name fails loudly.
#
#   topic                       producer            consumers
#   scan-events                 checkout (outbox)   analytics
#   transaction-completed       checkout (outbox)   inventory
#   transaction-completed.DLT   inventory           (people)
#   catalog-snapshots           catalog             checkout, inventory, analytics
#   reset-requested             admin               catalog
#   reset-completed             everyone else       admin
#
# One partition each. For scan-events that is a requirement: the popularity
# window is "the most recent 1000 scans", which needs a single global order,
# and Kafka orders records only within a partition.
#
# catalog-snapshots is compacted, so it always holds the latest catalog and a
# starting service reads one record instead of the whole history. A snapshot
# is one record holding the whole catalog, hence the 10 MB message limit.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: the Docker daemon is not running. Start Docker Desktop first." >&2
  exit 1
fi

docker compose up -d --wait --quiet-pull kafka

create() { # topic [--config k=v ...]
  local topic="$1"; shift
  docker exec selfcheckout-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1 "$@" >/dev/null
}

create scan-events
create transaction-completed
create transaction-completed.DLT
create catalog-snapshots \
  --config cleanup.policy=compact \
  --config max.message.bytes=10485760 \
  --config segment.ms=60000 \
  --config min.cleanable.dirty.ratio=0.01
create reset-requested
create reset-completed

echo "Kafka up on localhost:9092; topics:"
docker exec selfcheckout-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | sed 's/^/  /'
