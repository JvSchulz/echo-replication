#!/usr/bin/env bash
# Uma réplica. Uso: scripts/start-server.sh <id> [--registry host:porta] [--broker tcp://host:porta]
set -euo pipefail
cd "$(dirname "$0")/.."
if [ $# -lt 1 ]; then
  echo "uso: $0 <id> [--registry host:porta] [--broker tcp://host:porta]" >&2
  exit 1
fi
id="$1"
shift
exec java -cp target/echo.jar echo.server.ServerMain --id "$id" "$@"
