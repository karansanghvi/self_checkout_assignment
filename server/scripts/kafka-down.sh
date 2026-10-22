#!/usr/bin/env bash
# Stop the Kafka broker. Add -v to also delete its data volume.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down "$@"
