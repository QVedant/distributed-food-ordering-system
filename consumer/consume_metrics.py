
import json
import os
import sys
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "data" / "landed" / "order_metrics_landed.jsonl"

TOPIC = "order_metrics"
BOOTSTRAP_SERVERS = os.getenv("BS", "localhost:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "vm1-metrics-consumer")


def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
    )

    count = 0

    try:
        with OUTPUT_FILE.open("w", encoding="utf-8") as output:
            print(f"Listening to topic: {TOPIC}")
            print(f"Kafka brokers: {BOOTSTRAP_SERVERS}")
            print(f"Output file: {OUTPUT_FILE}")
            print("Waiting up to 10 seconds for messages...")

            for message in consumer:
                try:
                    record = json.loads(message.value.decode("utf-8"))
                    if not isinstance(record, dict):
                        print(
                            f"Skipping non-object JSON at offset {message.offset}",
                            file=sys.stderr,
                        )
                        continue

                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    output.flush()
                    count += 1

                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    print(
                        f"Skipping invalid message at offset {message.offset}: {exc}",
                        file=sys.stderr,
                    )

        consumer.commit()

    except KafkaError as exc:
        print(f"Kafka error: {exc}", file=sys.stderr)
        sys.exit(1)

    finally:
        consumer.close()

    print(f"Consumer finished. Landed {count} records.")


if __name__ == "__main__":
    main()
