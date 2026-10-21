import os
import sys
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "order.jsonl"
TOPIC = "orders"

BOOTSTRAP_SERVERS = os.getenv("BS", "localhost:9092")


def extract_order_id(raw_line):
    """Extract an order ID for the Kafka key when the JSON is valid."""
    import json

    try:
        record = json.loads(raw_line)
        if isinstance(record, dict) and record.get("order_id") is not None:
            return str(record["order_id"])
    except (json.JSONDecodeError, TypeError):
        pass

    return None


def main():
    if not INPUT_FILE.is_file():
        print(f"Input dataset not found: {INPUT_FILE}", file=sys.stderr)
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        acks="all",
        retries=5,
    )

    sent = 0
    failed = 0

    try:
        with INPUT_FILE.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                raw_line = line.strip()

                if not raw_line:
                    continue

                key = extract_order_id(raw_line)
                key_bytes = key.encode("utf-8") if key else None

                try:
                    producer.send(
                        TOPIC,
                        key=key_bytes,
                        value=raw_line.encode("utf-8"),
                    ).get(timeout=30)
                    sent += 1

                except (KafkaError, TimeoutError) as exc:
                    failed += 1
                    print(
                        f"Failed to publish line {line_number}: {exc}",
                        file=sys.stderr,
                    )

        producer.flush()

    finally:
        producer.close()

    print(f"Topic: {TOPIC}")
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
    print(f"Records published: {sent}")
    print(f"Records failed: {failed}")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
