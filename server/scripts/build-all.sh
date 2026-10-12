#!/usr/bin/env bash
# Build every module, run the tests, and produce one fat jar per deployable:
#   services/*/target/*-exec.jar  and  gateway/target/gateway-1.0.0-exec.jar
#
#   ./build-all.sh              # with tests
#   ./build-all.sh -DskipTests
set -euo pipefail
source "$(dirname "$0")/services.sh"
use_jdk21

cd "$SERVER_DIR"
mvn -B -q install "$@"

echo "Built:"
for entry in "${SERVICES[@]}"; do
  read -r name dir _ _ <<<"$entry"
  printf "  %-18s %s\n" "$name" "$(jar_for "$dir")"
done
