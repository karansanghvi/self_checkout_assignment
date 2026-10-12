#!/usr/bin/env bash
# The deployable units, in start order, with their ports and heap sizes.
# Sourced by build-all.sh, run-all.sh and stop-all.sh.
#
# admin-service comes first because it alone runs the Flyway migrations; the
# gateway comes last so the load client cannot reach it before its routes work.

SERVICES=(
  # name               module dir                     port  heap
  "admin-service       services/admin-service         8085  256m"
  "catalog-service     services/catalog-service       8081  256m"
  "checkout-service    services/checkout-service      8082  1g"
  "inventory-service   services/inventory-service     8083  256m"
  "analytics-service   services/analytics-service     8084  512m"
  "gateway             gateway                        8080  512m"
)

SERVER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$SERVER_DIR/run"

use_jdk21() {
  if ! JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null)"; then
    echo "ERROR: no JDK 21 found." >&2
    exit 1
  fi
  export JAVA_HOME
  export PATH="$JAVA_HOME/bin:$PATH"
}

jar_for() { echo "$SERVER_DIR/$1/target/$(basename "$1")-1.0.0-exec.jar"; }
