# Distributed Food Ordering System

## Project Overview
A distributed food-ordering analytics pipeline using Apache Kafka, KRaft, Apache Spark, and ZeroTier across four virtual machines.

## VM1 Responsibilities
- Producer 1: Publish raw orders to the Kafka `orders` topic.
- Consumer 2: Consume enriched records from `order_metrics` and save them locally.
- Spark Job 2: Generate restaurant and city analytics CSV files from the local landed data.

## Project Structure
- `producer/produce_orders.py` — Raw order producer.
- `consumer/consume_metrics.py` — Enriched metrics consumer.
- `spark/job2_operations_analytics.py` — Restaurant and city analytics.
- `data/input/` — Input data directory.
- `data/landed/` — Locally consumed JSONL files.
- `data/processed/` — Generated processing outputs.
- `config/` — Configuration templates.
- `scripts/` — Setup and operational scripts.

## Configuration
Copy `config/.env.example` to `.env` when local environment-file loading is implemented. Configure `BS` with the appropriate Kafka bootstrap address for the environment.

## Environment
- Ubuntu 24.04 LTS
- Java 11
- Apache Kafka 3.9.2
- Apache Spark 3.3.4
- Python 3.10 for Spark

## Required Outputs
- `order_metrics_landed.jsonl`
- `restaurant_summary.csv`
- `city_summary.csv`

## Important Constraints
- Preserve the original `order.jsonl` dataset.
- Spark jobs must read from local landing files, not directly from Kafka.
- Do not use HDFS for the assignment pipeline.
- The final Kafka topics require two partitions and replication factor two.

## Status
VM1 implementation is in progress. Functional testing and distributed integration will be performed after all components are implemented.