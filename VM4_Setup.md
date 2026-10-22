# VM4 Setup --- Tadi (Consumer 1, Spark Job 1, Producer 2)

This guide prepares VM4 for the Distributed Food Ordering System. VM4
consumes raw orders from Kafka, runs Spark Job 1, and publishes enriched
metrics back to Kafka.

## 1. VM4 details

-   Team member: Tadi
-   ZeroTier IP: `192.168.195.127`
-   Roles: Consumer 1, Spark Job 1, Producer 2
-   Kafka version used by the cluster: Apache Kafka 3.9.2
-   Spark version: Apache Spark 3.3.4
-   Expected Spark installation: `/opt/spark`

## 2. Clone or update the repository

``` bash
git clone <REPOSITORY_URL>
cd Assignment2
```

Replace `<REPOSITORY_URL>` with the team's actual GitHub repository URL.
If already cloned:

``` bash
cd Assignment2
git pull
```

Check that the expected files exist:

``` bash
ls config/settings.py
ls config/.env.example
ls requirements.txt
ls spark/job1_delivery_metrics.py
ls producer/produce_metrics.py
ls consumer/consume_orders.py
```

## 3. Install prerequisites

``` bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip openjdk-11-jdk
```

Verify:

``` bash
git --version
python3 --version
java -version
```

Install ZeroTier if necessary, join the team's existing network, and
have the network administrator authorize VM4. Confirm its ZeroTier IP:

``` bash
ip -br addr
```

The expected ZeroTier IP is `192.168.195.127`.

## 4. Create the Python environment

From the repository root:

``` bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Verify the dependencies:

``` bash
python -c "import kafka, dotenv; print('Python dependencies OK')"
```

Activate the virtual environment in each new terminal session:

``` bash
cd Assignment2
source .venv/bin/activate
```

## 5. Install or verify Spark 3.3.4

VM4 runs Spark Job 1. Install or verify Apache Spark 3.3.4 at
`/opt/spark`, following the same installation approach used by VM1.

``` bash
/opt/spark/bin/spark-submit --version
```

Set environment variables for the current terminal. Use a Python version
compatible with Spark 3.3.4; the example path may need adjustment for
VM4:

``` bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/opt/spark
export PATH="$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
```

Check the actual Python path and version:

``` bash
which python3
python3 --version
```

Use the same compatible Python executable for `PYSPARK_PYTHON` and
`PYSPARK_DRIVER_PYTHON`. VM1 currently uses `/usr/bin/python3.10`;
verify VM4 independently rather than assuming its Python version is
identical.

Do not install an unrelated PySpark version with `pip` simply to clear a
VS Code warning.

## 6. Create the local `.env` file

``` bash
cp config/.env.example .env
```

Edit `.env` and verify the team settings:

``` dotenv
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
SPARK_MASTER=local[*]
```

Keep the other settings from `config/.env.example`, including the file
paths and consumer group IDs. Check that the order consumer and metrics
consumer use their intended separate consumer groups.

The `.env` file is local configuration and must not be committed to Git.

## 7. Prepare local directories

``` bash
mkdir -p data/landed data/processed
```

VM4 uses these files:

-   Raw orders landing file: `data/landed/orders_landed.jsonl`
-   Spark Job 1 output: `data/processed/order_metrics.jsonl`

The original input dataset on VM1 is `order.jsonl`; VM4 does not need to
modify it.

## 8. Wait for Kafka cluster verification

Do not start the application consumer, producer, or Spark job until the
team confirms that VM2 and VM3 are running and both topics exist with
the intended configuration.

The expected topics are: - `orders` - `order_metrics`

Each should have two partitions and replication factor 2. The team
should inspect topic descriptions and ISR before running the pipeline.

## 9. VM4 pipeline steps

Run these steps in order after the Kafka cluster has been verified.

### Step 1 --- Start the order consumer

On VM4:

``` bash
source .venv/bin/activate
python consumer/consume_orders.py
```

Leave the consumer running while VM1 publishes the original orders.

### Step 2 --- Wait for VM1 to publish orders

The VM1 owner runs:

``` bash
source .venv/bin/activate
python producer/produce_orders.py
```

After the producer finishes, stop the VM4 order consumer gracefully with
Ctrl+C. Confirm the landing file exists:

``` bash
ls -lh data/landed/orders_landed.jsonl
```

### Step 3 --- Run Spark Job 1

On VM4:

``` bash
spark-submit --master 'local[*]' spark/job1_delivery_metrics.py
```

Expected output:

``` text
data/processed/order_metrics.jsonl
```

Check that it exists and contains JSONL records before continuing.

### Step 4 --- Publish metrics

On VM4:

``` bash
source .venv/bin/activate
python producer/produce_metrics.py
```

VM4 publishes the processed metrics to the Kafka topic `order_metrics`.
Do not transfer the metrics file directly to VM1.

## 10. VM4 checklist

-   [ ] Repository cloned or updated
-   [ ] ZeroTier connected and IP confirmed as `192.168.195.127`
-   [ ] Java and compatible Python installed
-   [ ] `.venv` created and requirements installed
-   [ ] Spark 3.3.4 installed and verified
-   [ ] Spark Python environment configured
-   [ ] `.env` created and team settings reviewed
-   [ ] `data/landed` and `data/processed` directories created
-   [ ] Kafka cluster and topics verified before running the pipeline
-   [ ] Spark Job 1 output checked
-   [ ] Metrics published to `order_metrics`

## Safety reminders

-   Do not commit `.env`.
-   Do not modify the original `order.jsonl` on VM1.
-   Do not run the pipeline before the team confirms Kafka cluster
    health.
-   Do not copy the metrics file directly to VM1; the assignment
    requires publishing it through Kafka.
