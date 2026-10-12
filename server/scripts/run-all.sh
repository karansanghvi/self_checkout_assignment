#!/usr/bin/env bash
# Start all six processes in the background, each from its own jar, and wait
# until every one reports healthy. Logs and PIDs go to server/run/.
#
#   ./run-all.sh
#   RESET_ON_STARTUP=true ./run-all.sh      # admin-service wipes + reseeds at boot
#
# Stop them with ./stop-all.sh.
set -euo pipefail
cd "$(dirname "$0")"
source ./db-env.sh
source ./services.sh
use_jdk21

mkdir -p "$RUN_DIR/logs"

wait_healthy() { # name port
  local name="$1" port="$2"
  for _ in $(seq 1 120); do
    if curl -fsS "http://localhost:$port/actuator/health" 2>/dev/null | grep -q '"UP"'; then
      printf "  %-18s UP on :%s\n" "$name" "$port"
      return 0
    fi
    sleep 0.5
  done
  echo "ERROR: $name did not become healthy on :$port. Last log lines:" >&2
  tail -20 "$RUN_DIR/logs/$name.log" >&2
  exit 1
}

start() { # name dir port heap
  local name="$1" dir="$2" port="$3" heap="$4" jar
  jar="$(jar_for "$dir")"
  if [ ! -f "$jar" ]; then
    echo "ERROR: $jar not found. Run ./build-all.sh first." >&2
    exit 1
  fi
  if [ -f "$RUN_DIR/$name.pid" ] && kill -0 "$(cat "$RUN_DIR/$name.pid")" 2>/dev/null; then
    echo "ERROR: $name is already running (pid $(cat "$RUN_DIR/$name.pid")). Run ./stop-all.sh first." >&2
    exit 1
  fi
  local extra=()
  if [ "$name" = "admin-service" ] && [ "${RESET_ON_STARTUP:-false}" = "true" ]; then
    extra+=(--app.reset-on-startup=true)
  fi
  nohup java "-Xmx$heap" -jar "$jar" --server.port="$port" ${extra[@]+"${extra[@]}"} \
      >"$RUN_DIR/logs/$name.log" 2>&1 &
  echo $! >"$RUN_DIR/$name.pid"
}

echo "Starting services..."

# 1. admin-service alone, so the schema is migrated before anyone reads it.
read -r name dir port heap <<<"${SERVICES[0]}"
start "$name" "$dir" "$port" "$heap"
wait_healthy "$name" "$port"

# 2. The four other domain services, in parallel.
for entry in "${SERVICES[@]:1:4}"; do
  read -r name dir port heap <<<"$entry"
  start "$name" "$dir" "$port" "$heap"
done
for entry in "${SERVICES[@]:1:4}"; do
  read -r name dir port heap <<<"$entry"
  wait_healthy "$name" "$port"
done

# 3. The gateway, last.
read -r name dir port heap <<<"${SERVICES[5]}"
start "$name" "$dir" "$port" "$heap"
wait_healthy "$name" "$port"

echo "All services up. Entry point: http://localhost:8080   Logs: $RUN_DIR/logs/"
