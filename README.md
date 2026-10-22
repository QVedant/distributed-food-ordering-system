# Distributed Food Ordering System

**Course:** Big Data  
**Technologies:** Apache Kafka 3.9.2, Apache Spark 3.3.4, KRaft, Python, ZeroTier  
**Architecture:** Four VMs · Two Kafka brokers · Two Spark batch jobs

## 1. Project Overview

This project implements a distributed food-ordering data pipeline using Apache Kafka for distributed messaging and Apache Spark for batch processing and analytics.

The pipeline processes raw food-order records, calculates delivery metrics, and generates restaurant-level and city-level performance summaries.

### Objectives

- Configure a multi-broker Kafka cluster using KRaft.
- Use Kafka topics, partitions, replication, and ISR to demonstrate distributed messaging and broker-failure behavior.
- Process raw order data using Spark Batch.
- Publish enriched order metrics through Kafka.
- Generate restaurant and city analytics from locally landed data.

## 2. Team and VM Assignments

| VM | Team member | ZeroTier IP | Responsibilities |
|---|---|---|---|
| VM1 | Vedant | `192.168.195.166` | Producer 1, Consumer 2, Spark Job 2 |
| VM2 | Pranay | `192.168.195.21` | Kafka Broker 1 and KRaft Controller |
| VM3 | Hemaksh | `192.168.195.167` | Kafka Broker 2 |
| VM4 | Tadi | `192.168.195.127` | Consumer 1, Spark Job 1, Producer 2 |

All VMs must join the same authorized ZeroTier network and use the same Kafka cluster.

## 3. Architecture and Data Flow

### Stage 1 — Order Enrichment

1. VM1 publishes raw records from `order.jsonl` to Kafka topic `orders`.
2. VM4 consumes the records and saves them locally as `data/landed/orders_landed.jsonl`.
3. VM4 runs Spark Job 1 on the landed file.
4. Spark Job 1 cleans and enriches the valid orders, producing `data/processed/order_metrics.jsonl`.
5. VM4 publishes the enriched records to Kafka topic `order_metrics`.

### Stage 2 — Operations Analytics

1. VM1 consumes `order_metrics` and saves records as `data/landed/order_metrics_landed.jsonl`.
2. VM1 runs Spark Job 2 on the landed file.
3. Spark generates `restaurant_summary.csv` and `city_summary.csv`.

**Important design constraints:**
- Spark must not consume Kafka directly.
- HDFS must not be used; Spark reads and writes local files.
- The metrics file must be published through Kafka, not transferred directly from VM4 to VM1.
- Both Kafka topics must use the same broker cluster.
- The original `order.jsonl` dataset must remain unchanged.

## 4. Software Prerequisites

Install the following on the indicated machines:

| Software | VM1 | VM2 | VM3 | VM4 |
|---|---|---|---|---|
| Git | Required | Required | Required | Required |
| ZeroTier | Required | Required | Required | Required |
| Java | Required | Required | Required | Required |
| Python 3 | Required | Required | Required | Required |
| Apache Kafka 3.9.2 | Client tools | Broker/controller | Broker | Client tools |
| Apache Spark 3.3.4 | Job 2 | Not required | Not required | Job 1 |

The application is intended for Linux environments. These instructions assume Bash and Linux-style installation paths. Adjust paths if a VM uses a different installation directory.

### Verify installations

Run the relevant commands on each VM:

```bash
java -version
python3 --version
git --version
```

On VM1 and VM4:

```bash
echo "$SPARK_HOME"
spark-submit --version
```

On all Kafka-related machines:

```bash
echo "$KAFKA_HOME"
"$KAFKA_HOME/bin/kafka-topics.sh" --version
```

The current VM1 development environment uses Java 11, Kafka 3.9.2, and Spark 3.3.4. Confirm compatible Java and Python configurations on the other VMs before starting the cluster.

## 5. Clone and Prepare the Repository

Every team member should clone the shared GitHub repository:

```bash
git clone <REPOSITORY_URL>
cd Assignment2
```

Replace `<REPOSITORY_URL>` with the team's actual repository URL.

If the repository is already cloned, update it from the shared branch:

```bash
git pull
```

Avoid running `git pull` while you have uncommitted changes that may conflict with teammates' work.

### Python virtual environment

Run on VM1 and VM4:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

For each new terminal session:

```bash
cd Assignment2
source .venv/bin/activate
```

Ensure the repository's `requirements.txt` contains the Python packages used by the project, including `kafka-python` and `python-dotenv` if imported by the application.

## 6. Repository Structure

```text
Assignment2/
├── config/
│   ├── __init__.py
│   ├── settings.py
│   ├── .env.example
│   ├── kafka-vm2.properties.example
│   └── kafka-vm3.properties.example
├── consumer/
│   ├── consume_orders.py
│   └── consume_metrics.py
├── data/
│   ├── input/
│   ├── landed/
│   └── processed/
├── producer/
│   ├── produce_orders.py
│   └── produce_metrics.py
├── scripts/
│   ├── reset_vm1_demo.sh
│   ├── run_vm1_analytics.sh
│   ├── run_vm4_pipeline.sh
│   ├── start_kafka_vm2.sh
│   └── start_kafka_vm3.sh
├── spark/
│   ├── job1_delivery_metrics.py
│   └── job2_operations_analytics.py
├── order.jsonl
├── requirements.txt
├── .gitignore
└── README.md
```

The directory structure above describes the intended project layout. If a file is absent in the current repository checkout, confirm it has been committed and pulled before following its commands.

## 7. Environment Configuration

Create a local `.env` file in the repository root on VM1 and VM4:

```bash
cp config/.env.example .env
```

Edit the values to match the team's configuration.

### Team cluster settings

Use these shared values:

```dotenv
DEPLOYMENT_MODE=team

VM1_ZT_IP=192.168.195.166
VM2_ZT_IP=192.168.195.21
VM3_ZT_IP=192.168.195.167
VM4_ZT_IP=192.168.195.127

KAFKA_PORT=9092
BS=192.168.195.21:9092,192.168.195.167:9092

ORDERS_TOPIC=orders
METRICS_TOPIC=order_metrics
KAFKA_PARTITIONS=2
KAFKA_REPLICATION_FACTOR=2
KAFKA_ACKS=all

KAFKA_AUTO_OFFSET_RESET=earliest
SPARK_MASTER=local[*]
```

Keep the other settings from `.env.example`, including consumer group IDs and file paths. VM1 and VM4 may use different consumer group IDs for their separate consumers.

### Local-only development

For a single-broker local Kafka test, use:

```dotenv
DEPLOYMENT_MODE=local
BS=localhost:9092
KAFKA_PARTITIONS=2
KAFKA_REPLICATION_FACTOR=1
SPARK_MASTER=local[*]
```

Do not use replication factor 2 against a single-broker local cluster.

### Configuration files and Git

Commit the example files:

- `config/.env.example`
- `config/kafka-vm2.properties.example`
- `config/kafka-vm3.properties.example`

Keep local `.env` and the actual VM-specific Kafka property files out of Git. Confirm `.gitignore` contains:

```gitignore
.env
.venv/
__pycache__/
*.py[cod]
*.log
config/kafka-vm2.properties
config/kafka-vm3.properties
data/landed/*.jsonl
data/processed/*
!data/landed/.gitkeep
!data/processed/.gitkeep
restaurant_summary.csv
city_summary.csv
```

Do not ignore `order.jsonl`, since it is the source dataset.

## 8. Kafka Cluster Configuration

Kafka uses KRaft mode. VM2 is the controller and Broker 1; VM3 is Broker 2.

The configuration templates are:

- `config/kafka-vm2.properties.example`
- `config/kafka-vm3.properties.example`

On VM2, create the local configuration:

```bash
cp config/kafka-vm2.properties.example config/kafka-vm2.properties
```

On VM3:

```bash
cp config/kafka-vm3.properties.example config/kafka-vm3.properties
```

Verify that each configuration contains the correct `node.id`, advertised listener IP, controller voter address, and local storage directory.

The current templates assume Kafka is installed at `/opt/kafka`. Adjust the path in the startup script if necessary.

### Initialize KRaft storage

**Do this only once, during the coordinated team setup. Do not run it until both VM configurations and storage paths have been reviewed.**

On VM2, generate a cluster ID:

```bash
"$KAFKA_HOME/bin/kafka-storage.sh" random-uuid
```

Record the generated ID and share it with the team.

On VM2, format its storage using the generated ID:

```bash
"$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "<SHARED_CLUSTER_ID>" \
  -c config/kafka-vm2.properties
```

On VM3, use exactly the same cluster ID but its own configuration:

```bash
"$KAFKA_HOME/bin/kafka-storage.sh" format \
  -t "<SHARED_CLUSTER_ID>" \
  -c config/kafka-vm3.properties
```

Replace `<SHARED_CLUSTER_ID>` with the actual generated value.

Do not format storage that has already been initialized without first reviewing its state. Formatting is a destructive or state-changing initialization operation and should be coordinated.

### Start Kafka

Start VM2 first:

```bash
./scripts/start_kafka_vm2.sh
```

Then start VM3:

```bash
./scripts/start_kafka_vm3.sh
```

Run each startup command in its own terminal. Keep the terminals open while the brokers run.

These commands are the planned startup procedure; the cluster has not yet been initialized or connection-tested.

### Create topics

Once both brokers are running and the cluster is confirmed healthy, execute on a machine with Kafka client tools:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server 192.168.195.21:9092 \
  --create \
  --topic orders \
  --partitions 2 \
  --replication-factor 2
```

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server 192.168.195.21:9092 \
  --create \
  --topic order_metrics \
  --partitions 2 \
  --replication-factor 2
```

If a topic already exists, inspect it instead of deleting or recreating it blindly.

List topics:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server 192.168.195.21:9092 \
  --list
```

Describe topic partitions and replicas:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server 192.168.195.21:9092 \
  --describe \
  --topic orders
```

Repeat for `order_metrics`.

## 9. Spark Configuration

Spark runs locally on VM1 and VM4. No Spark cluster manager or HDFS is required for the intended pipeline.

Set the environment variables for each VM's local installation. For example:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/opt/spark
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
```

Adjust the Python executable to the version supported by the installed Spark release. On VM1, the current working setup uses `/usr/bin/python3.10`.

Verify Spark:

```bash
spark-submit --version
```

Run Spark jobs with the repository's configured `SPARK_MASTER`, normally `local[*]`.

## 10. Running the Pipeline

Run the stages in the specified order. Keep the Kafka broker processes running.

### Step 1 — VM4: Start the order consumer

On VM4:

```bash
source .venv/bin/activate
python consumer/consume_orders.py
```

The consumer writes incoming order records to:

```text
data/landed/orders_landed.jsonl
```

Start this consumer before publishing orders from VM1.

### Step 2 — VM1: Publish the original dataset

On VM1:

```bash
source .venv/bin/activate
python producer/produce_orders.py
```

The producer reads `order.jsonl` and publishes its records to Kafka topic `orders`. The original dataset must not be modified.

Wait for the producer to finish. Then stop the VM4 order consumer gracefully so the landing file is complete for batch processing.

### Step 3 — VM4: Run Spark Job 1

On VM4:

```bash
spark-submit --master 'local[*]' spark/job1_delivery_metrics.py
```

Input:

```text
data/landed/orders_landed.jsonl
```

Output:

```text
data/processed/order_metrics.jsonl
```

### Step 4 — VM4: Publish enriched metrics

After Spark Job 1 completes:

```bash
source .venv/bin/activate
python producer/produce_metrics.py
```

The producer publishes the enriched records to Kafka topic `order_metrics`. Do not copy the processed file directly to VM1.

### Step 5 — VM1: Start the metrics consumer

On VM1:

```bash
source .venv/bin/activate
python consumer/consume_metrics.py
```

The consumer writes records to:

```text
data/landed/order_metrics_landed.jsonl
```

Start the consumer before publishing metrics from VM4. Once all metrics have arrived, stop the consumer gracefully.

### Step 6 — VM1: Run Spark Job 2

On VM1:

```bash
spark-submit --master 'local[*]' spark/job2_operations_analytics.py
```

Expected output files:

```text
restaurant_summary.csv
city_summary.csv
```

The helper scripts `scripts/run_vm4_pipeline.sh` and `scripts/run_vm1_analytics.sh` may also be used once their configured paths and execution behavior have been reviewed.

## 11. Data Cleaning and Transformations

### Spark Job 1 — Order Enrichment

The raw input schema contains:

- `order_id`
- `customer_id`
- `restaurant_id`
- `delivery_partner_id`
- `city`
- `order_timestamp`
- `accepted_timestamp`
- `picked_up_timestamp`
- `expected_delivery_timestamp`
- `delivered_timestamp`
- `distance_km`
- `order_amount`
- `delivery_fee`
- `status`

Ignore records with malformed JSON, missing or invalid required fields, invalid required timestamps, `distance_km <= 0`, `order_amount <= 0`, or a status other than `DELIVERED`.

Exact duplicate orders must be counted only once.

Calculate:

- `preparation_time_min` = picked-up timestamp − accepted timestamp
- `delivery_time_min` = delivered timestamp − picked-up timestamp
- `fulfillment_time_min` = delivered timestamp − order timestamp
- `delivery_speed_kmph` = distance in kilometres ÷ delivery time in hours
- `delay_min` = maximum of zero and (delivered timestamp − expected delivery timestamp)

Assign `delivery_status` as `ON_TIME` when the delay is zero and `DELAYED` otherwise.

Assign `delay_category` as follows:

| Delay | Category |
|---|---|
| 0 minutes | `ON_TIME` |
| 1–15 minutes | `SLIGHT_DELAY` |
| 16–30 minutes | `MODERATE_DELAY` |
| More than 30 minutes | `SEVERE_DELAY` |

### Spark Job 2 — Operations Analytics

Use the enriched metrics landed on VM1. Exact duplicate records must be counted only once.

**Restaurant analytics** — group by `restaurant_id` and calculate:

- `order_count`
- `avg_order_amount`
- `avg_preparation_time_min`
- `avg_delivery_time_min`
- `delayed_order_count`
- `delay_rate_pct`

**City analytics** — group by `city` and calculate:

- `order_count`
- `avg_order_amount`
- `avg_delivery_time_min`
- `delayed_order_count`
- `delay_rate_pct`

The delay rate is:

`delay_rate_pct = 100 × delayed_order_count / order_count`

Round calculated numerical values to two decimal places.

## 12. Expected Files and Outputs

| File or topic | Location | Purpose |
|---|---|---|
| `order.jsonl` | VM1 | Original input dataset |
| `orders` | Kafka cluster | Raw order messages |
| `orders_landed.jsonl` | VM4 | Locally consumed raw orders |
| `order_metrics.jsonl` | VM4 | Spark Job 1 enriched output |
| `order_metrics` | Kafka cluster | Enriched order messages |
| `order_metrics_landed.jsonl` | VM1 | Locally consumed metrics |
| `restaurant_summary.csv` | VM1 | Restaurant-level analytics |
| `city_summary.csv` | VM1 | City-level analytics |

## 13. Broker-Failure Demonstration

The assignment's broker-failure test must stop **VM3**, not VM2. VM2 hosts the only controller in this design, and controller handover is outside the intended demonstration.

Before the test:

1. Confirm both brokers are running.
2. Describe both Kafka topics.
3. Verify their replicas and in-sync replicas (ISR).
4. Confirm that replication factor 2 has been applied.

Then stop the Kafka broker process on VM3 and observe the effect on the replicated partitions and message availability through VM2. Record the topic and ISR state during the test.

Afterward, restart VM3 and verify that it rejoins the cluster and replicas return to a healthy state.

Do not assume successful failover solely because a producer or consumer command returns successfully; record the topic and ISR details as evidence.

## 14. Troubleshooting

### Kafka command not found

Check the installation path:

```bash
echo "$KAFKA_HOME"
ls "$KAFKA_HOME/bin"
```

### Spark command not found

```bash
echo "$SPARK_HOME"
ls "$SPARK_HOME/bin/spark-submit"
```

Ensure the Spark `bin` directory is on `PATH`.

### Python dependency errors

Activate the virtual environment and install dependencies:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### No records in a landing file

Check that the consumer was started before the producer, that the topic and bootstrap servers are correct, and that the consumer has not exited.

### Spark input file not found

Check that the relevant consumer has completed and that the expected landing file exists:

```bash
ls -lh data/landed/
```

### Kafka replication errors

Verify that both brokers are running, that their configurations use the same cluster ID and controller quorum, and that the topic has the intended replication factor and ISR.

### Avoid accidental data loss

Do not delete Kafka log directories or reformat storage to resolve a connection issue. Inspect the existing cluster state first.

## 15. Current Setup Status

The application code and team configuration templates are being prepared. The VM assignments and ZeroTier addresses are known.

The following steps remain pending:

- Confirm prerequisites and installation paths on all VMs.
- Review Spark Job 1 against the team's actual dataset.
- Complete and review the helper scripts.
- Generate a shared KRaft cluster ID and initialize broker storage.
- Start the cluster and create topics.
- Perform connection, end-to-end pipeline, and broker-failure tests.

This README describes the intended setup and execution workflow. It should be updated with actual test results after the team validates the cluster.
