#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
CONFIG_FILE="$PROJECT_ROOT/config/kafka-vm3.properties"

if [[ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
    echo "Error: Kafka startup script not found or not executable:"
    echo "  $KAFKA_HOME/bin/kafka-server-start.sh"
    exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Missing Kafka configuration: $CONFIG_FILE"
    echo "Create it from config/kafka-vm3.properties.example."
    exit 1
fi

echo "Starting Kafka broker for VM3..."
echo "Kafka home: $KAFKA_HOME"
echo "Configuration: $CONFIG_FILE"

exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG_FILE"