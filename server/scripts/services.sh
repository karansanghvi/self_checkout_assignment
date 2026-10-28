#!/usr/bin/env bash
# The deployable units, in start order, with their ports and heap sizes.
# Sourced by build-all.sh, run-all.sh, stop-all.sh and create-db.sh.
#
# Services only talk through Kafka, so none needs another to be up in order
# to start. catalog-service still goes first so its startup snapshot is on
# the topic before the others read it; the gateway comes last so the load
# client cannot reach it before its routes work.

SERVICES=(
  # name               module dir                     port  heap
  "catalog-service     services/catalog-service       8081  256m"
  "inventory-service   services/inventory-service     8083  512m"
  "analytics-service   services/analytics-service     8084  512m"
  "checkout-service    services/checkout-service      8082  1g"
  "admin-service       services/admin-service         8085  256m"
  "gateway             gateway                        8080  512m"
)

# One database per service that owns data, and the only role that may open it.
SERVICE_DATABASES=(
  # database      role
  "catalog_db     catalog_svc"
  "inventory_db   inventory_svc"
  "checkout_db    checkout_svc"
  "analytics_db   analytics_svc"
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
