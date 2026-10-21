
import json
import os
import sys
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "order.jsonl"
TOPIC = "orders"

BOOTSTRAP_SERVERS = os.getenv("BS", "localhost:9092")


def json_serializer(record):
    return json.dumps(record, separators=(",", ":")).encode("utf-8")


def main():
    if not INPUT_FILE.is_file():
        print(f"ERROR: Input file not found: {INPUT_FILE}", file=sys.stderr)
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=json_serializer,
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
    )

    sent = 0
    failed = 0

    try:
        with INPUT_FILE.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(
                        f"Skipping malformed JSON at line {line_number}: {exc}",
                        file=sys.stderr,
                    )
                    failed += 1
                    continue

                if not isinstance(record, dict):
                    print(
                        f"Skipping non-object JSON at line {line_number}",
                        file=sys.stderr,
                    )
                    failed += 1
                    continue

                order_id = record.get("order_id")
                key = str(order_id) if order_id is not None else None

                try:
                    producer.send(TOPIC, key=key, value=record).get(timeout=30)
                    sent += 1
                except (KafkaError, TimeoutError) as exc:
                    print(
                        f"Failed to publish line {line_number}: {exc}",
                        file=sys.stderr,
                    )
                    failed += 1

        producer.flush()
    finally:
        producer.close()

    print(f"Producer finished. Published: {sent}; skipped/failed: {failed}")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
