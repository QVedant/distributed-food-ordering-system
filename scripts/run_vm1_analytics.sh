#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${SPARK_HOME:-}" ]]; then
    echo "Error: SPARK_HOME is not set."
    exit 1
fi

INPUT_FILE="$PROJECT_ROOT/data/landed/order_metrics_landed.jsonl"

if [[ ! -f "$INPUT_FILE" ]]; then
    echo "Error: Metrics input file not found: $INPUT_FILE"
    echo "Run the VM1 metrics consumer first."
    exit 1
fi

mkdir -p "$PROJECT_ROOT/data/processed"

spark-submit \
    --master local[*] \
    "$PROJECT_ROOT/spark/job2_operations_analytics.py"