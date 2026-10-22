import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")


def get_setting(name, default=None):
    return os.getenv(name, default)


def get_int_setting(name, default, minimum=1):
    raw_value = get_setting(name, str(default))

    try:
        value = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{name} must be an integer; received {raw_value!r}"
        ) from exc

    if value < minimum:
        raise ValueError(
            f"{name} must be at least {minimum}; received {value}"
        )

    return value


def get_path_setting(name, default):
    path = Path(get_setting(name, default)).expanduser()

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    return path


DEPLOYMENT_MODE = get_setting(
    "DEPLOYMENT_MODE", "local"
).strip().lower()

if DEPLOYMENT_MODE not in {"local", "team"}:
    raise ValueError(
        "DEPLOYMENT_MODE must be either 'local' or 'team'."
    )


VM1_ZT_IP = get_setting("VM1_ZT_IP", "CHANGE_ME").strip()
VM2_ZT_IP = get_setting("VM2_ZT_IP", "CHANGE_ME").strip()
VM3_ZT_IP = get_setting("VM3_ZT_IP", "CHANGE_ME").strip()
VM4_ZT_IP = get_setting("VM4_ZT_IP", "CHANGE_ME").strip()

KAFKA_PORT = get_int_setting("KAFKA_PORT", 9092)
BS = get_setting("BS", "localhost:9092").strip()

if not BS:
    raise ValueError("BS must contain at least one Kafka bootstrap server.")

BOOTSTRAP_SERVERS = [
    server.strip()
    for server in BS.split(",")
    if server.strip()
]

if not BOOTSTRAP_SERVERS:
    raise ValueError("No valid Kafka bootstrap servers were configured.")

if DEPLOYMENT_MODE == "team":
    if any(
        ip == "CHANGE_ME"
        for ip in (VM1_ZT_IP, VM2_ZT_IP, VM3_ZT_IP, VM4_ZT_IP)
    ):
        raise ValueError(
            "Configure all VM ZeroTier IPs for team deployment."
        )

    if BS == "localhost:9092":
        raise ValueError(
            "Team deployment cannot use BS=localhost:9092."
        )


ORDERS_TOPIC = get_setting("ORDERS_TOPIC", "orders").strip()
METRICS_TOPIC = get_setting("METRICS_TOPIC", "order_metrics").strip()

if not ORDERS_TOPIC or not METRICS_TOPIC:
    raise ValueError("Kafka topic names must not be empty.")

KAFKA_PARTITIONS = get_int_setting("KAFKA_PARTITIONS", 2)
KAFKA_REPLICATION_FACTOR = get_int_setting(
    "KAFKA_REPLICATION_FACTOR", 2
)

KAFKA_ACKS = get_setting("KAFKA_ACKS", "all").strip()

if KAFKA_ACKS not in {"all", "1", "0"}:
    raise ValueError("KAFKA_ACKS must be 'all', '1', or '0'.")


KAFKA_GROUP_ID = get_setting(
    "KAFKA_GROUP_ID", "vm1-metrics-consumer"
).strip()

KAFKA_AUTO_OFFSET_RESET = get_setting(
    "KAFKA_AUTO_OFFSET_RESET", "earliest"
).strip()

if KAFKA_AUTO_OFFSET_RESET not in {"earliest", "latest", "none"}:
    raise ValueError(
        "KAFKA_AUTO_OFFSET_RESET must be earliest, latest, or none."
    )


ORDERS_INPUT_FILE = get_path_setting(
    "ORDERS_INPUT_FILE", "order.jsonl"
)

ORDERS_LANDED_FILE = get_path_setting(
    "ORDERS_LANDED_FILE", "data/landed/orders_landed.jsonl"
)

METRICS_LANDED_FILE = get_path_setting(
    "METRICS_LANDED_FILE",
    "data/landed/order_metrics_landed.jsonl",
)

METRICS_PROCESSED_FILE = get_path_setting(
    "METRICS_PROCESSED_FILE",
    "data/processed/order_metrics.jsonl",
)

RESTAURANT_OUTPUT_FILE = get_path_setting(
    "RESTAURANT_OUTPUT_FILE", "restaurant_summary.csv"
)

CITY_OUTPUT_FILE = get_path_setting(
    "CITY_OUTPUT_FILE", "city_summary.csv"
)

SPARK_MASTER = get_setting("SPARK_MASTER", "local[*]").strip()

if not SPARK_MASTER:
    raise ValueError("SPARK_MASTER must not be empty.")
