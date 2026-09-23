#!/usr/bin/env bash
# Run the reference load client against this server.
#
# The client is compiled at class-file version 65 and uses virtual threads, so
# it needs a JDK 21+. The machine default is 17, hence the explicit JAVA_HOME.
#
#   ./run-load-test.sh                    # 10 stations, 60s (assignment default)
#   ./run-load-test.sh --stations=100 --duration=180
set -euo pipefail
cd "$(dirname "$0")"

CLIENT_DIR="../../reference-repo/load-client"
REPORT_DIR="${REPORT_DIR:-$(cd .. && pwd)/reports}"

if ! JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null)"; then
  echo "ERROR: no JDK 21 found. Install one, or edit this script to point at 21+." >&2
  exit 1
fi
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
echo "Using JDK: $(java -version 2>&1 | head -1)"

mkdir -p "$REPORT_DIR"

# Rebuild the client so we are never running stale classes.
(cd "$CLIENT_DIR" && ./build.sh >/dev/null)

(cd "$CLIENT_DIR" && ./run.sh \
    --baseUrl="${BASE_URL:-http://localhost:8080}" \
    --reportDir="$REPORT_DIR" \
    "$@")

echo
echo "Reports in: $REPORT_DIR"
