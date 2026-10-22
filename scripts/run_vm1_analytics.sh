#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${SPARK_HOME:-}" ]]; then
    echo "Error: SPARK_HOME is not set."
    exit 1
fi

if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
    echo "Error: spark-submit not found or not executable at:"
    echo "  $SPARK_HOME/bin/spark-submit"
    exit 1
fi

if [[ ! -f "$PROJECT_ROOT/.env" ]]; then
    echo "Error: .env file not found."
    echo "Create it with: cp config/.env.example .env"
    exit 1
fi

set -a
source "$PROJECT_ROOT/.env"
set +a

INPUT_FILE="${METRICS_LANDED_FILE:-data/landed/order_metrics_landed.jsonl}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"

if [[ "$INPUT_FILE" != /* ]]; then
    INPUT_FILE="$PROJECT_ROOT/$INPUT_FILE"
fi

if [[ ! -f "$INPUT_FILE" ]]; then
    echo "Error: Metrics input file not found: $INPUT_FILE"
    echo "Run the VM1 metrics consumer first."
    exit 1
fi

mkdir -p "$PROJECT_ROOT/data/processed"

echo "Running Spark Job 2"
echo "Metrics input: $INPUT_FILE"
echo "Spark master: $SPARK_MASTER"

"$SPARK_HOME/bin/spark-submit" \
    --master "$SPARK_MASTER" \
    "$PROJECT_ROOT/spark/job2_operations_analytics.py"