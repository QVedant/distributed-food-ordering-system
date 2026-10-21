import json
import sys

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.settings import (
    BS,
    KAFKA_ACKS,
    ORDERS_INPUT_FILE,
    ORDERS_TOPIC,
)


def json_serializer(record):
    return json.dumps(
        record, separators=(",", ":")
    ).encode("utf-8")


def main():
    if not ORDERS_INPUT_FILE.is_file():
        print(f"Input dataset not found: {ORDERS_INPUT_FILE}")
        sys.exit(1)

    producer = None
    sent = 0
    failed = 0
    skipped = 0

    try:
        producer = KafkaProducer(
            bootstrap_servers=BS.split(","),
            value_serializer=json_serializer,
            key_serializer=lambda key: (
                key.encode("utf-8") if key is not None else None
            ),
            acks=KAFKA_ACKS,
            retries=5,
        )

        print(f"Input file: {ORDERS_INPUT_FILE}")
        print(f"Topic: {ORDERS_TOPIC}")
        print(f"Bootstrap servers: {BS}")

        with ORDERS_INPUT_FILE.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                line = line.strip()

                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    print(
                        f"Skipping malformed JSON at line {line_number}"
                    )
                    skipped += 1
                    continue

                if not isinstance(record, dict):
                    print(
                        f"Skipping non-object JSON at line {line_number}"
                    )
                    skipped += 1
                    continue

                order_id = record.get("order_id")
                key = str(order_id) if order_id is not None else None

                try:
                    producer.send(
                        ORDERS_TOPIC,
                        key=key,
                        value=record,
                    ).get(timeout=30)
                    sent += 1
                except (KafkaError, TimeoutError) as exc:
                    print(
                        f"Failed to publish line {line_number}: {exc}"
                    )
                    failed += 1

        producer.flush()

    except KafkaError as exc:
        print(f"Kafka connection or publishing error: {exc}")
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
