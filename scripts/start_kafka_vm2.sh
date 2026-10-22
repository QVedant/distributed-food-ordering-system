#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
CONFIG_FILE="$PROJECT_ROOT/config/kafka-vm2.properties"

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Missing config: $CONFIG_FILE"
    exit 1
fi

exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG_FILE"