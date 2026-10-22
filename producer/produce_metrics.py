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
    METRICS_PROCESSED_FILE,
    METRICS_TOPIC,
)


def main():
    if not METRICS_PROCESSED_FILE.is_file():
        print(
            f"Input file not found: {METRICS_PROCESSED_FILE}",
            file=sys.stderr,
        )
        sys.exit(1)

    producer = None
    sent = 0
    failed = 0
    skipped = 0

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            key_serializer=lambda key: (
                key.encode("utf-8") if key is not None else None
            ),
            value_serializer=lambda value: value.encode("utf-8"),
            acks=KAFKA_ACKS,
            retries=5,
        )

        print(f"Input file: {METRICS_PROCESSED_FILE}")
        print(f"Topic: {METRICS_TOPIC}")
        print(f"Bootstrap servers: {', '.join(BOOTSTRAP_SERVERS)}")

        with METRICS_PROCESSED_FILE.open(
            "r",
            encoding="utf-8",
        ) as file:
            for line_number, line in enumerate(file, start=1):
                record_line = line.rstrip("\r\n")

                if not record_line.strip():
                    skipped += 1
                    continue

                try:
                    record = json.loads(record_line)

                    if not isinstance(record, dict):
                        skipped += 1
                        continue

                    order_id = record.get("order_id")
                    key = str(order_id) if order_id is not None else None

                except (json.JSONDecodeError, TypeError):
                    print(
                        f"Skipping invalid JSON at line {line_number}",
                        file=sys.stderr,
                    )
                    skipped += 1
                    continue

                try:
                    producer.send(
                        METRICS_TOPIC,
                        key=key,
                        value=record_line,
                    ).get(timeout=30)
                    sent += 1

                except (KafkaError, TimeoutError) as exc:
                    print(
                        f"Failed to publish line {line_number}: {exc}",
                        file=sys.stderr,
                    )
                    failed += 1

        producer.flush()

    except KafkaError as exc:
        print(f"Kafka publishing error: {exc}", file=sys.stderr)
        sys.exit(1)

    finally:
        if producer is not None:
            producer.close()

    print(f"Records published: {sent}")
    print(f"Records skipped: {skipped}")
    print(f"Records failed: {failed}")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()