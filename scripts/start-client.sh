#!/usr/bin/env bash
# Cliente interativo. Uso: scripts/start-client.sh [--registry host:porta]
set -euo pipefail
cd "$(dirname "$0")/.."
exec java -cp target/echo.jar echo.client.ClientMain "$@"
