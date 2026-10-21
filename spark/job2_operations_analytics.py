
import os
import shutil
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "landed" / "order_metrics_landed.jsonl"

RESTAURANT_OUTPUT = PROJECT_ROOT / "restaurant_summary.csv"
CITY_OUTPUT = PROJECT_ROOT / "city_summary.csv"

SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("delivery_partner_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("preparation_time_min", DoubleType(), True),
    StructField("delivery_time_min", DoubleType(), True),
    StructField("fulfillment_time_min", DoubleType(), True),
    StructField("delivery_speed_kmph", DoubleType(), True),
    StructField("delay_min", DoubleType(), True),
    StructField("delivery_status", StringType(), True),
    StructField("delay_category", StringType(), True),
])


def write_single_csv(dataframe, output_file):
    """Write a single CSV file with a header and the required filename."""
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    temp_dir = tempfile.mkdtemp(
        prefix=f".{output_file.stem}_",
        dir=str(output_file.parent),
    )

    try:
        dataframe.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("emptyValue", "") \
            .csv(temp_dir)

        part_files = list(Path(temp_dir).glob("part-*.csv"))
        if not part_files:
            raise RuntimeError(f"No CSV part file generated for {output_file}")

        shutil.move(str(part_files[0]), str(output_file))
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    if not INPUT_FILE.is_file():
        print(f"ERROR: Input file not found: {INPUT_FILE}", file=sys.stderr)
        sys.exit(1)

    spark = (
        SparkSession.builder
        .appName("VM1-Operations-Analytics")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = spark.read.text(str(INPUT_FILE))

        parsed = raw.select(
            F.from_json(F.col("value"), SCHEMA).alias("record")
        ).select("record.*")

        # Keep complete records and count exact duplicate records only once.
        required_columns = [
            "order_id",
            "restaurant_id",
            "city",
            "order_amount",
            "preparation_time_min",
            "delivery_time_min",
            "delivery_status",
        ]

        valid = parsed.dropna(subset=required_columns)

        valid = valid.filter(
            (F.trim(F.col("order_id")) != "")
            & (F.trim(F.col("restaurant_id")) != "")
            & (F.trim(F.col("city")) != "")
            & F.col("order_amount").isNotNull()
            & F.col("preparation_time_min").isNotNull()
            & F.col("delivery_time_min").isNotNull()
            & F.col("delivery_status").isin("ON_TIME", "DELAYED")
        )

        valid = valid.dropDuplicates()

        # Restaurant performance.
        restaurant_summary = (
            valid.groupBy("restaurant_id")
            .agg(
                F.count("*").alias("order_count"),
                F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
                F.round(
                    F.avg("preparation_time_min"), 2
                ).alias("avg_preparation_time_min"),
                F.round(
                    F.avg("delivery_time_min"), 2
                ).alias("avg_delivery_time_min"),
                F.sum(
                    F.when(F.col("delivery_status") == "DELAYED", 1)
                    .otherwise(0)
                ).alias("delayed_order_count"),
            )
            .withColumn(
                "delay_rate_pct",
                F.round(
                    F.col("delayed_order_count") * 100.0
                    / F.col("order_count"),
                    2,
                ),
            )
            .select(
                "restaurant_id",
                "order_count",
                "avg_order_amount",
                "avg_preparation_time_min",
                "avg_delivery_time_min",
                "delayed_order_count",
                "delay_rate_pct",
            )
            .orderBy("restaurant_id")
        )

        # City delivery performance.
        city_summary = (
            valid.groupBy("city")
            .agg(
                F.count("*").alias("order_count"),
                F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
                F.round(
                    F.avg("delivery_time_min"), 2
                ).alias("avg_delivery_time_min"),
                F.sum(
                    F.when(F.col("delivery_status") == "DELAYED", 1)
                    .otherwise(0)
                ).alias("delayed_order_count"),
            )
            .withColumn(
                "delay_rate_pct",
                F.round(
                    F.col("delayed_order_count") * 100.0
                    / F.col("order_count"),
                    2,
                ),
            )
            .select(
                "city",
                "order_count",
                "avg_order_amount",
                "avg_delivery_time_min",
                "delayed_order_count",
                "delay_rate_pct",
            )
            .orderBy("city")
        )

        write_single_csv(restaurant_summary, RESTAURANT_OUTPUT)
        write_single_csv(city_summary, CITY_OUTPUT)

        print(f"Valid unique orders processed: {valid.count()}")
        print(f"Restaurant summary: {RESTAURANT_OUTPUT}")
        print(f"City summary: {CITY_OUTPUT}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
