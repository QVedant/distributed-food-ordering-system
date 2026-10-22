#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${SPARK_HOME:-}" ]]; then
    echo "Error: SPARK_HOME is not set."
    exit 1
fi

if [[ ! -f "$PROJECT_ROOT/.env" ]]; then
    echo "Error: .env file not found."
    exit 1
fi

echo "VM4 pipeline"
echo "1. Start Consumer 1 in a separate terminal:"
echo "   python \"$PROJECT_ROOT/consumer/consume_orders.py\""
echo
echo "2. Stop Consumer 1 with Ctrl+C after the orders have landed."
echo "3. Run Spark Job 1:"
echo "   \"$SPARK_HOME/bin/spark-submit\" --master \"${SPARK_MASTER:-local[*]}\" \"$PROJECT_ROOT/spark/job1_delivery_metrics.py\""
echo "4. Publish enriched records:"
echo "   python \"$PROJECT_ROOT/producer/produce_metrics.py\""
echo
echo "This script prints the run order; it does not execute the pipeline automatically."
