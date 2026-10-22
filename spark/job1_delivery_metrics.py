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


def write_single_jsonl(dataframe, output_file):
    output_file = Path(output_file)
    temp_dir = output_file.parent / f"{output_file.stem}_spark_output"

    output_file.parent.mkdir(parents=True, exist_ok=True)

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
        raise RuntimeError(
            f"No JSON part file generated for {output_file.name}"
        )

    output_file.unlink(missing_ok=True)
    shutil.move(str(part_files[0]), str(output_file))
    shutil.rmtree(temp_dir)


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
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_data = spark.read.text(str(ORDERS_LANDED_FILE))

        parsed_data = raw_data.select(
            from_json(col("value"), ORDERS_SCHEMA).alias("record")
        ).select("record.*")

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
                to_timestamp(col(column_name)),
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

        cleaned_data = parsed_data.dropna(subset=required_columns)

        cleaned_data = cleaned_data.filter(
            (col("order_id") != "")
            & (col("restaurant_id") != "")
            & (col("city") != "")
            & (col("distance_km") > 0)
            & (col("order_amount") > 0)
            & (col("status") == "DELIVERED")
        )

        cleaned_data = cleaned_data.dropDuplicates()

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
                        - unix_timestamp("expected_delivery_timestamp")
                    ) / 60.0,
                ),
            )
        )

        enriched_data = enriched_data.filter(
            (col("preparation_time_min") >= 0)
            & (col("delivery_time_min") >= 0)
            & (col("fulfillment_time_min") >= 0)
        )

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
            when(col("delay_min") == 0, "ON_TIME").otherwise("DELAYED"),
        )

        enriched_data = enriched_data.withColumn(
            "delay_category",
            when(col("delay_min") == 0, "ON_TIME")
            .when(col("delay_min") <= 15, "SLIGHT_DELAY")
            .when(col("delay_min") <= 30, "MODERATE_DELAY")
            .otherwise("SEVERE_DELAY"),
        )

        enriched_data = enriched_data.select(
            "order_id",
            "customer_id",
            "restaurant_id",
            "delivery_partner_id",
            "city",
            "order_amount",
            "delivery_fee",
            "distance_km",
            spark_round("preparation_time_min", 2).alias(
                "preparation_time_min"
            ),
            spark_round("delivery_time_min", 2).alias(
                "delivery_time_min"
            ),
            spark_round("fulfillment_time_min", 2).alias(
                "fulfillment_time_min"
            ),
            spark_round("delivery_speed_kmph", 2).alias(
                "delivery_speed_kmph"
            ),
            spark_round("delay_min", 2).alias("delay_min"),
            "delivery_status",
            "delay_category",
        )

        write_single_jsonl(enriched_data, METRICS_PROCESSED_FILE)

        print(f"Enriched records saved to: {METRICS_PROCESSED_FILE}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
