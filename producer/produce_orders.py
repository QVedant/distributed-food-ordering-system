import sys
import json
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    BOOTSTRAP_SERVERS,
    KAFKA_ACKS,
    ORDERS_INPUT_FILE,
    ORDERS_TOPIC,
)


def main():
    if not ORDERS_INPUT_FILE.is_file():
        print(f"Input dataset not found: {ORDERS_INPUT_FILE}", file=sys.stderr)
        sys.exit(1)

    producer = None
    sent = 0
    failed = 0
    skipped = 0

    try:
        producer = KafkaProducer(
            bootstrap_servers=[
                "192.168.195.21:9092",
                "192.168.195.167:9092",
            ],
            key_serializer=lambda key: (
                key.encode("utf-8") if key is not None else None
            ),
            value_serializer=lambda value: value.encode("utf-8"),
            acks="all",
            retries=10,
            retry_backoff_ms=1000,
            request_timeout_ms=30000,
            max_block_ms=60000,
        )

        print(f"Input file: {ORDERS_INPUT_FILE}")
        print(f"Topic: {ORDERS_TOPIC}")
        print(f"Bootstrap servers: {', '.join(BOOTSTRAP_SERVERS)}")

        with ORDERS_INPUT_FILE.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                line = line.rstrip("\r\n")

                if not line.strip():
                    skipped += 1
                    continue

                key = None

                try:
                    record = json.loads(line)
                    if isinstance(record, dict):
                        order_id = record.get("order_id")
                        if order_id is not None:
                            key = str(order_id)
                except (json.JSONDecodeError, TypeError):
                    pass

                try:
                    producer.send(
                        ORDERS_TOPIC,
                        key=key,
                        value=line,
                    ).get(timeout=60)
                    sent += 1
                except (KafkaError, TimeoutError) as exc:
                    print(
                        f"Failed to publish line {line_number}: {exc}",
                        file=sys.stderr,
                    )
                    failed += 1

        producer.flush()

    except KafkaError as exc:
        print(f"Kafka connection or publishing error: {exc}", file=sys.stderr)
        sys.exit(1)

    finally:
        if producer is not None:
            producer.close()

    print(f"Records published: {sent}")
    print(f"Blank lines skipped: {skipped}")
    print(f"Records failed: {failed}")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()