# Distributed Food Ordering System

## Project Overview
A distributed food-ordering analytics pipeline using Apache Kafka, KRaft, Apache Spark, and ZeroTier across four virtual machines.

## VM1 — Assigned Responsibilities

### Producer 1
- Reads the original `order.jsonl` dataset.
- Publishes raw records to Kafka topic `orders`.
- Uses the `BS` environment variable for the Kafka bootstrap address.

### Consumer 2
- Consumes enriched records from Kafka topic `order_metrics`.
- Saves received records to `data/landed/order_metrics_landed.jsonl`.

### Spark Job 2 — Operations Analytics
- Reads the locally landed metrics file.
- Generates `restaurant_summary.csv` with restaurant-level statistics.
- Generates `city_summary.csv` with city-level statistics.
- Rounds calculated numerical values to two decimal places.

### Integration Dependencies
- VM2 hosts Kafka Broker 1 and the KRaft controller.
- VM3 hosts Kafka Broker 2.
- VM4 consumes raw orders, runs Spark Job 1, and publishes enriched records.
- VM1 and VM4 must use the same Kafka cluster through ZeroTier.


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




## Local Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd Assignment2
```

### 2. Create your environment file

Copy the shared template to the project root:

```bash
cp config/.env.example .env
```

Edit `.env` and configure the values for your VM and deployment environment.

- For local development, use `DEPLOYMENT_MODE=local` and `BS=localhost:9092`.
- For team integration, use `DEPLOYMENT_MODE=team` and set `BS` to the reachable Kafka broker address on the ZeroTier network.
- Fill in the VM ZeroTier IP addresses when they become available.

### 3. Create and activate a Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 4. Install Python dependencies

```bash
python -m pip install -r requirements.txt
```

### 5. Check the configuration

The application loads `.env` through `config/settings.py`. Keep your personal `.env` out of Git and commit changes to `.env.example` when shared configuration defaults change.
