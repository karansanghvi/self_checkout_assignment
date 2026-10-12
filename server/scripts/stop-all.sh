#!/usr/bin/env bash
# Stop everything run-all.sh started, gateway first.
set -uo pipefail
source "$(dirname "$0")/services.sh"

for (( i=${#SERVICES[@]}-1; i>=0; i-- )); do
  read -r name _ _ _ <<<"${SERVICES[$i]}"
  pidfile="$RUN_DIR/$name.pid"
  [ -f "$pidfile" ] || continue
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid"
    printf "  %-18s stopped\n" "$name"
  fi
  rm -f "$pidfile"
done
