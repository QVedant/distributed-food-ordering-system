import json
import signal
import sys
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    BOOTSTRAP_SERVERS,
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_GROUP_ID,
    METRICS_LANDED_FILE,
    METRICS_TOPIC,
)

running = True


def stop_consumer(signum, frame):
    global running
    running = False


def main():
    global running
    running = True

    METRICS_LANDED_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    signal.signal(signal.SIGINT, stop_consumer)
    signal.signal(signal.SIGTERM, stop_consumer)

    consumer = None
    count = 0

    try:
        consumer = KafkaConsumer(
            METRICS_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
            enable_auto_commit=False,
        )

        print(f"Topic: {METRICS_TOPIC}")
        print(f"Bootstrap servers: {', '.join(BOOTSTRAP_SERVERS)}")
        print(f"Consumer group: {KAFKA_GROUP_ID}")
        print(f"Output file: {METRICS_LANDED_FILE}")
        print("Consumer is running. Press Ctrl+C to stop.")

        with METRICS_LANDED_FILE.open(
            "a",
            encoding="utf-8",
        ) as output:
            while running:
                batches = consumer.poll(timeout_ms=1000)

                for _, messages in batches.items():
                    for message in messages:
                        try:
                            record = json.loads(
                                message.value.decode("utf-8")
                            )

                            if not isinstance(record, dict):
                                print(
                                    "Skipping non-object JSON message."
                                )
                                continue

                            output.write(
                                json.dumps(
                                    record,
                                    separators=(",", ":"),
                                )
                                + "\n"
                            )
                            count += 1

                        except (
                            UnicodeDecodeError,
                            json.JSONDecodeError,
                        ):
                            print("Skipping invalid JSON message.")

                # Flush landed records before committing their offsets.
                output.flush()

                if batches and running:
                    consumer.commit()

    except (KafkaError, OSError) as exc:
        print(
            f"Metrics consumer failed: {exc}",
            file=sys.stderr,
        )
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
