#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Resetting generated VM1 outputs..."

if pgrep -af '[c]onsume_metrics.py' >/dev/null; then
    echo "Stop consume_metrics.py before resetting outputs."
    exit 1
fi

find "$PROJECT_ROOT/data/landed" \
    -maxdepth 1 -type f \
    ! -name ".gitkeep" \
    -delete

find "$PROJECT_ROOT/data/processed" \
    -mindepth 1 -maxdepth 1 \
    ! -name ".gitkeep" \
    -exec rm -rf -- {} +

rm -f -- \
    "$PROJECT_ROOT/restaurant_summary.csv" \
    "$PROJECT_ROOT/city_summary.csv"

echo "VM1 generated outputs cleared."
echo "Original order.jsonl was not modified."
