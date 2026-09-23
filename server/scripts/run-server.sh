#!/usr/bin/env bash
# Start the Spring Boot server with DB settings loaded from .env.
#
#   ./run-server.sh
#   ./run-server.sh --app.reset-on-startup=true
set -euo pipefail
cd "$(dirname "$0")"
source ./db-env.sh

if ! JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null)"; then
  echo "ERROR: no JDK 21 found." >&2
  exit 1
fi
export JAVA_HOME

cd ..
exec mvn -B spring-boot:run -Dspring-boot.run.jvmArguments="-Xmx1g" \
     ${1:+-Dspring-boot.run.arguments="$*"}
