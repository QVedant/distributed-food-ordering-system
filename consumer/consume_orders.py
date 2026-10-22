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
    ORDERS_LANDED_FILE,
    ORDERS_TOPIC,
)

running = True


def stop_consumer(signum, frame):
    global running
    running = False


def main():
    global running

    ORDERS_LANDED_FILE.parent.mkdir(parents=True, exist_ok=True)

    signal.signal(signal.SIGINT, stop_consumer)
    signal.signal(signal.SIGTERM, stop_consumer)

    consumer = None
    count = 0

    try:
        consumer = KafkaConsumer(
            ORDERS_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id="vm4-orders-consumer",
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
            enable_auto_commit=False,
        )

        print(f"Topic: {ORDERS_TOPIC}")
        print(f"Bootstrap servers: {', '.join(BOOTSTRAP_SERVERS)}")
        print(f"Output file: {ORDERS_LANDED_FILE}")
        print("Consumer running. Press Ctrl+C to stop.")

        with ORDERS_LANDED_FILE.open("a", encoding="utf-8") as output:
            while running:
                batches = consumer.poll(timeout_ms=1000)

                for _, messages in batches.items():
                    for message in messages:
                        output.write(
                            message.value.decode(
                                "utf-8",
                                errors="replace",
                            )
                            + "\n"
                        )
                        count += 1

                output.flush()

                if not running:
                    break

                if batches:
                    consumer.commit()

    except KafkaError as exc:
        print(f"Kafka consumer error: {exc}", file=sys.stderr)
        sys.exit(1)

    finally:
        if consumer is not None:
            consumer.close()

    print(f"Records landed during this session: {count}")


if __name__ == "__main__":
    main()