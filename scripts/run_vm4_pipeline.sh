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
    echo "Copy config/.env.example to .env and configure it first."
    exit 1
fi

echo "VM4 pipeline — manual run order"
echo
echo "Prerequisites:"
echo "  - Activate the project's Python virtual environment."
echo "  - Ensure Kafka is running and VM4 can reach the brokers."
echo
echo "1. Start Consumer 1 in a separate terminal:"
echo "   python \"$PROJECT_ROOT/consumer/consume_orders.py\""
echo
echo "2. After the orders have landed, stop Consumer 1 with Ctrl+C."
echo
echo "3. Run Spark Job 1:"
echo "   \"$SPARK_HOME/bin/spark-submit\" --master \"${SPARK_MASTER:-local[*]}\" \"$PROJECT_ROOT/spark/job1_delivery_metrics.py\""
echo
echo "4. Publish the enriched records:"
echo "   python \"$PROJECT_ROOT/producer/produce_metrics.py\""
echo
echo "This script prints the run order; it does not execute the pipeline automatically."
