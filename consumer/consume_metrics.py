import json
import os
import signal
import sys
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "data" / "landed" / "order_metrics_landed.jsonl"

TOPIC = "order_metrics"
BOOTSTRAP_SERVERS = os.getenv("BS", "localhost:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "vm1-metrics-consumer")

running = True


def stop_consumer(signum, frame):
    global running
    running = False


def main():
    global running

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    signal.signal(signal.SIGINT, stop_consumer)
    signal.signal(signal.SIGTERM, stop_consumer)

    consumer = None
    count = 0

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            key_deserializer=lambda key: (
                key.decode("utf-8") if key is not None else None
            ),
            consumer_timeout_ms=None,
        )

        # Append so restarting the consumer does not erase previous output.
        with OUTPUT_FILE.open("a", encoding="utf-8") as output:
            print(f"Consuming topic: {TOPIC}")
            print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
            print(f"Output file: {OUTPUT_FILE}")
            print("Press Ctrl+C to stop.")

            while running:
                records = consumer.poll(timeout_ms=1000)

                for _, messages in records.items():
                    for message in messages:
                        if not running:
                            break

                        try:
                            raw_value = message.value.decode("utf-8")
                            record = json.loads(raw_value)

                            if not isinstance(record, dict):
                                print("Skipping non-object JSON message.")
                                continue

                            output.write(
                                json.dumps(record, separators=(",", ":")) + "\n"
                            )
                            output.flush()
                            count += 1

                        except (UnicodeDecodeError, json.JSONDecodeError):
                            print("Skipping invalid JSON message.")
                            continue

                # Commit offsets only after successfully processing the polled batch.
                consumer.commit()

    except KafkaError as exc:
        print(f"Kafka consumer error: {exc}", file=sys.stderr)
        sys.exit(1)

    finally:
        if consumer is not None:
            try:
                consumer.close()
            except KafkaError:
                pass

    print(f"Records written during this session: {count}")


if __name__ == "__main__":
    main()
