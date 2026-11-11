#!/usr/bin/env bash
# Serviço de nomes. Precisa das interfaces remotas no classpath (rode ./mvnw package antes).
# Uso: scripts/start-registry.sh [porta]
set -euo pipefail
cd "$(dirname "$0")/.."
exec rmiregistry -J-Djava.class.path=target/classes "${1:-1099}"
