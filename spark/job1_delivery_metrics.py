import sys
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    unix_timestamp,
    when,
    greatest,
    lit,
    round as spark_round,
    trim,
    countDistinct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    ORDERS_LANDED_FILE,
    METRICS_PROCESSED_FILE,
    SPARK_MASTER,
)

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("delivery_partner_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("accepted_timestamp", StringType(), True),
    StructField("picked_up_timestamp", StringType(), True),
    StructField("expected_delivery_timestamp", StringType(), True),
    StructField("delivered_timestamp", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("status", StringType(), True),
])


def parse_timestamp(column_name):
    return to_timestamp(
        col(column_name),
        TIMESTAMP_FORMAT,
    )


def write_single_jsonl(dataframe, output_file):
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    temp_dir = output_file.parent / (
        f"{output_file.stem}_spark_output"
    )

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    (
        dataframe.coalesce(1)
        .write
        .mode("overwrite")
        .json(str(temp_dir))
    )

    part_files = list(temp_dir.glob("part-*.json"))

    if not part_files:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(
            f"No JSON part file generated for {output_file.name}"
        )

    try:
        shutil.move(str(part_files[0]), str(output_file))
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    if not ORDERS_LANDED_FILE.is_file():
        print(
            f"Input file not found: {ORDERS_LANDED_FILE}",
            file=sys.stderr,
        )
        sys.exit(1)

    spark = (
        SparkSession.builder
        .appName("DistributedFoodOrdering-DeliveryMetrics")
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read JSONL as text so malformed JSON lines can be rejected.
        raw_data = spark.read.text(str(ORDERS_LANDED_FILE))

        parsed_data = (
            raw_data
            .select(
                from_json(
                    col("value"),
                    ORDERS_SCHEMA,
                ).alias("record")
            )
            .select("record.*")
        )

        timestamp_columns = [
            "order_timestamp",
            "accepted_timestamp",
            "picked_up_timestamp",
            "expected_delivery_timestamp",
            "delivered_timestamp",
        ]

        for column_name in timestamp_columns:
            parsed_data = parsed_data.withColumn(
                column_name,
                parse_timestamp(column_name),
            )

        required_columns = [
            "order_id",
            "customer_id",
            "restaurant_id",
            "delivery_partner_id",
            "city",
            "order_timestamp",
            "accepted_timestamp",
            "picked_up_timestamp",
            "expected_delivery_timestamp",
            "delivered_timestamp",
            "distance_km",
            "order_amount",
            "delivery_fee",
            "status",
        ]

        # Reject records missing any required field or valid timestamp.
        cleaned_data = parsed_data.dropna(
            subset=required_columns
        )

        string_columns = [
            "order_id",
            "customer_id",
            "restaurant_id",
            "delivery_partner_id",
            "city",
            "status",
        ]

        for column_name in string_columns:
            cleaned_data = cleaned_data.withColumn(
                column_name,
                trim(col(column_name)),
            )

        # Keep only valid, delivered orders.
        cleaned_data = cleaned_data.filter(
            (col("order_id") != "")
            & (col("customer_id") != "")
            & (col("restaurant_id") != "")
            & (col("delivery_partner_id") != "")
            & (col("city") != "")
            & (col("status") == "DELIVERED")
            & (col("distance_km") > 0)
            & (col("order_amount") > 0)
            & (
                unix_timestamp("accepted_timestamp")
                >= unix_timestamp("order_timestamp")
            )
        )

        # Reject records with impossible event chronology.
        cleaned_data = cleaned_data.filter(
            (unix_timestamp("picked_up_timestamp")
             >= unix_timestamp("accepted_timestamp"))
            & (
                unix_timestamp("delivered_timestamp")
                >= unix_timestamp("picked_up_timestamp")
            )
            & (
                unix_timestamp("delivered_timestamp")
                >= unix_timestamp("order_timestamp")
            )
        )

        # Reject order IDs with conflicting valid records.
        # Exact duplicate rows are counted once.
        order_variants = (
            cleaned_data
            .groupBy("order_id")
            .agg(
                countDistinct(
                    "customer_id",
                    "restaurant_id",
                    "delivery_partner_id",
                    "city",
                    "order_timestamp",
                    "accepted_timestamp",
                    "picked_up_timestamp",
                    "expected_delivery_timestamp",
                    "delivered_timestamp",
                    "distance_km",
                    "order_amount",
                    "delivery_fee",
                    "status",
                ).alias("variant_count")
            )
            .filter(col("variant_count") == 1)
            .select("order_id")
        )

        cleaned_data = (
            cleaned_data
            .join(
                order_variants,
                on="order_id",
                how="inner",
            )
            .dropDuplicates(["order_id"])
        )

        # Calculate preparation, delivery, fulfillment, and delay.
        enriched_data = (
            cleaned_data
            .withColumn(
                "preparation_time_min",
                (
                    unix_timestamp("picked_up_timestamp")
                    - unix_timestamp("accepted_timestamp")
                ) / 60.0,
            )
            .withColumn(
                "delivery_time_min",
                (
                    unix_timestamp("delivered_timestamp")
                    - unix_timestamp("picked_up_timestamp")
                ) / 60.0,
            )
            .withColumn(
                "fulfillment_time_min",
                (
                    unix_timestamp("delivered_timestamp")
                    - unix_timestamp("order_timestamp")
                ) / 60.0,
            )
            .withColumn(
                "delay_min",
                greatest(
                    lit(0.0),
                    (
                        unix_timestamp("delivered_timestamp")
                        - unix_timestamp(
                            "expected_delivery_timestamp"
                        )
                    ) / 60.0,
                ),
            )
        )

        # Calculate average delivery speed; zero-duration deliveries
        # have undefined speed and are represented as null.
        enriched_data = enriched_data.withColumn(
            "delivery_speed_kmph",
            when(
                col("delivery_time_min") > 0,
                col("distance_km")
                / (col("delivery_time_min") / 60.0),
            ),
        )

        enriched_data = enriched_data.withColumn(
            "delivery_status",
            when(
                col("delay_min") == 0,
                "ON_TIME",
            ).otherwise("DELAYED"),
        )

        enriched_data = enriched_data.withColumn(
            "delay_category",
            when(
                col("delay_min") == 0,
                "ON_TIME",
            )
            .when(
                col("delay_min") <= 15,
                "SLIGHT_DELAY",
            )
            .when(
                col("delay_min") <= 30,
                "MODERATE_DELAY",
            )
            .otherwise("SEVERE_DELAY"),
        )

        # Select the required output schema and round calculated values.
        enriched_data = enriched_data.select(
            "order_id",
            "customer_id",
            "restaurant_id",
            "delivery_partner_id",
            "city",
            "order_amount",
            "delivery_fee",
            "distance_km",
            spark_round(
                "preparation_time_min", 2
            ).alias("preparation_time_min"),
            spark_round(
                "delivery_time_min", 2
            ).alias("delivery_time_min"),
            spark_round(
                "fulfillment_time_min", 2
            ).alias("fulfillment_time_min"),
            spark_round(
                "delivery_speed_kmph", 2
            ).alias("delivery_speed_kmph"),
            spark_round(
                "delay_min", 2
            ).alias("delay_min"),
            "delivery_status",
            "delay_category",
        )

        write_single_jsonl(
            enriched_data,
            METRICS_PROCESSED_FILE,
        )

        print(
            f"Enriched records saved to: {METRICS_PROCESSED_FILE}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
