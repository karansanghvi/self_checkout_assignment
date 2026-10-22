#!/usr/bin/env bash
# Start the Kafka broker (Docker) and create the scan-events topic.
#
# One partition, deliberately: the popularity window is "the most recent 1000
# scans", which needs a single global order, and Kafka orders records only
# within a partition.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: the Docker daemon is not running. Start Docker Desktop first." >&2
  exit 1
fi

docker compose up -d --wait --quiet-pull kafka

docker exec selfcheckout-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic scan-events --partitions 1 --replication-factor 1 >/dev/null

echo "Kafka up on localhost:9092; topic scan-events ready."
