import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")


def get_setting(name, default=None):
    return os.getenv(name, default)


DEPLOYMENT_MODE = get_setting("DEPLOYMENT_MODE", "local")

VM1_ZT_IP = get_setting("VM1_ZT_IP", "CHANGE_ME")
VM2_ZT_IP = get_setting("VM2_ZT_IP", "CHANGE_ME")
VM3_ZT_IP = get_setting("VM3_ZT_IP", "CHANGE_ME")
VM4_ZT_IP = get_setting("VM4_ZT_IP", "CHANGE_ME")

KAFKA_PORT = int(get_setting("KAFKA_PORT", "9092"))
BS = get_setting("BS", "localhost:9092")

ORDERS_TOPIC = get_setting("ORDERS_TOPIC", "orders")
METRICS_TOPIC = get_setting("METRICS_TOPIC", "order_metrics")

KAFKA_PARTITIONS = int(get_setting("KAFKA_PARTITIONS", "2"))
KAFKA_REPLICATION_FACTOR = int(
    get_setting("KAFKA_REPLICATION_FACTOR", "2")
)
KAFKA_ACKS = get_setting("KAFKA_ACKS", "all")

KAFKA_GROUP_ID = get_setting(
    "KAFKA_GROUP_ID", "vm1-metrics-consumer"
)
KAFKA_AUTO_OFFSET_RESET = get_setting(
    "KAFKA_AUTO_OFFSET_RESET", "earliest"
)

ORDERS_INPUT_FILE = PROJECT_ROOT / get_setting(
    "ORDERS_INPUT_FILE", "order.jsonl"
)
ORDERS_LANDED_FILE = PROJECT_ROOT / get_setting(
    "ORDERS_LANDED_FILE",
    "data/landed/orders_landed.jsonl",
)
METRICS_LANDED_FILE = PROJECT_ROOT / get_setting(
    "METRICS_LANDED_FILE",
    "data/landed/order_metrics_landed.jsonl",
)
METRICS_PROCESSED_FILE = PROJECT_ROOT / get_setting(
    "METRICS_PROCESSED_FILE",
    "data/processed/order_metrics.jsonl",
)

RESTAURANT_OUTPUT_FILE = PROJECT_ROOT / get_setting(
    "RESTAURANT_OUTPUT_FILE", "restaurant_summary.csv"
)
CITY_OUTPUT_FILE = PROJECT_ROOT / get_setting(
    "CITY_OUTPUT_FILE", "city_summary.csv"
)

SPARK_MASTER = get_setting("SPARK_MASTER", "local[*]")
