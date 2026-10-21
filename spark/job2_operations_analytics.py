import sys
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    lit,
    round as spark_round,
    sum as spark_sum,
    when,
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
    CITY_OUTPUT_FILE,
    METRICS_LANDED_FILE,
    RESTAURANT_OUTPUT_FILE,
    SPARK_MASTER,
)

INPUT_FILE = METRICS_LANDED_FILE
RESTAURANT_OUTPUT = RESTAURANT_OUTPUT_FILE
CITY_OUTPUT = CITY_OUTPUT_FILE
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

METRICS_SCHEMA = StructType([
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
    output_file = Path(output_file)
    temp_dir = OUTPUT_DIR / f"{output_file.stem}_spark_output"

    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_dir.parent.mkdir(parents=True, exist_ok=True)

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    (
        dataframe.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("encoding", "UTF-8")
        .csv(str(temp_dir))
    )

    part_files = list(temp_dir.glob("part-*.csv"))

    if not part_files:
        raise RuntimeError(
            f"No CSV part file generated for {output_file.name}"
        )

    output_file.unlink(missing_ok=True)
    shutil.move(str(part_files[0]), str(output_file))
    shutil.rmtree(temp_dir)


def main():
    if not INPUT_FILE.is_file():
        print(
            f"Input file not found: {INPUT_FILE}",
            file=sys.stderr,
        )
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("DistributedFoodOrdering-OperationsAnalytics")
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_data = spark.read.text(str(INPUT_FILE))

        parsed_data = raw_data.select(
            from_json(col("value"), METRICS_SCHEMA).alias("record")
        ).select("record.*")

        required_columns = [
            "order_id",
            "restaurant_id",
            "city",
            "order_amount",
            "preparation_time_min",
            "delivery_time_min",
            "delivery_status",
        ]

        cleaned_data = parsed_data.dropna(
            subset=required_columns
        )

        cleaned_data = cleaned_data.filter(
            (col("order_id") != "")
            & (col("restaurant_id") != "")
            & (col("city") != "")
            & (col("order_amount") > 0)
            & (col("preparation_time_min") >= 0)
            & (col("delivery_time_min") >= 0)
            & col("delivery_status").isin("ON_TIME", "DELAYED")
        )

        unique_orders = cleaned_data.dropDuplicates()

        delayed_count = spark_sum(
            when(
                col("delivery_status") == "DELAYED",
                1,
            ).otherwise(0)
        )

        restaurant_summary = unique_orders.groupBy(
            "restaurant_id"
        ).agg(
            count("*").alias("order_count"),
            spark_round(
                avg("order_amount"), 2
            ).alias("avg_order_amount"),
            spark_round(
                avg("preparation_time_min"), 2
            ).alias("avg_preparation_time_min"),
            spark_round(
                avg("delivery_time_min"), 2
            ).alias("avg_delivery_time_min"),
            delayed_count.alias("delayed_order_count"),
        )

        restaurant_summary = restaurant_summary.withColumn(
            "delay_rate_pct",
            spark_round(
                lit(100.0)
                * col("delayed_order_count")
                / col("order_count"),
                2,
            ),
        ).select(
            "restaurant_id",
            "order_count",
            "avg_order_amount",
            "avg_preparation_time_min",
            "avg_delivery_time_min",
            "delayed_order_count",
            "delay_rate_pct",
        )

        city_summary = unique_orders.groupBy("city").agg(
            count("*").alias("order_count"),
            spark_round(
                avg("order_amount"), 2
            ).alias("avg_order_amount"),
            spark_round(
                avg("delivery_time_min"), 2
            ).alias("avg_delivery_time_min"),
            delayed_count.alias("delayed_order_count"),
        )

        city_summary = city_summary.withColumn(
            "delay_rate_pct",
            spark_round(
                lit(100.0)
                * col("delayed_order_count")
                / col("order_count"),
                2,
            ),
        ).select(
            "city",
            "order_count",
            "avg_order_amount",
            "avg_delivery_time_min",
            "delayed_order_count",
            "delay_rate_pct",
        )

        write_single_csv(
            restaurant_summary,
            RESTAURANT_OUTPUT,
        )

        write_single_csv(
            city_summary,
            CITY_OUTPUT,
        )

        print(f"Restaurant analytics saved to: {RESTAURANT_OUTPUT}")
        print(f"City analytics saved to: {CITY_OUTPUT}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()