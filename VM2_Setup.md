# Distributed Food Ordering System --- Setup Guide for VM2, VM3, and VM4

This guide prepares the three remaining team VMs for the Distributed
Food Ordering System. Follow only the section for your assigned VM.

> **Important:** This guide covers installation and configuration
> preparation. Do not initialize/format Kafka storage or start the
> cluster until VM2 and VM3 have both been configured and the team has
> coordinated the one-time KRaft initialization. If Kafka storage may
> already have been initialized, inspect it first---do not reformat it
> blindly.

## 1. Team assignments and addresses

  -------------------------------------------------------------------------
  VM                Team member       ZeroTier IP         Role
  ----------------- ----------------- ------------------- -----------------
  VM1               Vedant            `192.168.195.166`   Producer 1,
                                                          Consumer 2, Spark
                                                          Job 2

  VM2               Pranay            `192.168.195.21`    Kafka Broker 1
                                                          and KRaft
                                                          Controller

  VM3               Hemaksh           `192.168.195.167`   Kafka Broker 2

  VM4               Tadi              `192.168.195.127`   Consumer 1, Spark
                                                          Job 1, Producer 2
  -------------------------------------------------------------------------

All four VMs must join the same authorized ZeroTier network. Use the IP
addresses above only after confirming that they are still assigned to
the intended devices.

## 2. Clone the repository

On each of VM2, VM3, and VM4, run:

``` bash
git clone <REPOSITORY_URL>
cd Assignment2
```

Replace `<REPOSITORY_URL>` with the team's actual repository URL.

If the repository is already cloned, update it instead:

``` bash
cd Assignment2
git pull
```

Do not pull over uncommitted changes that you need to keep. Confirm the
expected files exist:

``` bash
ls
ls config
ls scripts
```

## 3. Common prerequisites and ZeroTier

On all three VMs, install basic tools:

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

Install ZeroTier if it is not already installed, join the team's
existing network, and have the network administrator authorize the VM.
Confirm the assigned address:

``` bash
ip -br addr
```

The VM must have its expected ZeroTier IP before it is used in Kafka
configuration. If `ping` is allowed on the network, you can check
reachability; a failed ping alone does not prove Kafka connectivity is
broken because ICMP may be blocked.

------------------------------------------------------------------------

# Part A --- VM2: Pranay (Kafka Broker 1 + KRaft Controller)

## A1. Install or verify Kafka

The team uses Apache Kafka **3.9.2**. Use the same version on VM2 and
VM3. The existing team setup expects Kafka under `/opt/kafka`.

If Kafka is already installed, verify it:

``` bash
/opt/kafka/bin/kafka-topics.sh --version
```

If the installation path differs, adjust the commands and startup
scripts consistently. Do not reinstall over an existing installation
without checking it first.

## A2. Create the local broker configuration

From the repository root:

``` bash
cp config/kafka-vm2.properties.example config/kafka-vm2.properties
```

The local file `config/kafka-vm2.properties` should contain the
following important values:

``` properties
process.roles=broker,controller
node.id=2
controller.quorum.voters=2@192.168.195.21:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://192.168.195.21:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
```

Also inspect `log.dirs`. It must point to VM2's own intended local Kafka
storage directory. Keep the actual configuration file out of Git; the
example template is the file to commit.

Inspect the important settings:

``` bash
grep -E '^(process.roles|node.id|controller.quorum.voters|listeners|advertised.listeners|log.dirs)=' config/kafka-vm2.properties
```

Expected role and address details: - `node.id=2` -
`process.roles=broker,controller` - Controller voter
`2@192.168.195.21:9093` - Advertised broker address
`192.168.195.21:9092`

## A3. Do not initialize storage yet

Do **not** run `kafka-storage.sh format` or start Kafka until VM3 is
configured and both machines' storage state has been checked.

During the coordinated setup, the team must: 1. Confirm neither broker
has existing initialized storage that needs preserving. 2. Generate one
cluster ID. 3. Use the same cluster ID when formatting each broker's own
storage, if formatting is actually required. 4. Start VM2 first, then
VM3. 5. Verify broker and topic health before running the application
pipeline.

Do not generate separate cluster IDs for the two brokers.

------------------------------------------------------------------------

# Part B --- VM3: Hemaksh (Kafka Broker 2)

## B1. Install or verify Kafka

Install or verify Kafka **3.9.2** at `/opt/kafka`, matching VM2.

``` bash
/opt/kafka/bin/kafka-topics.sh --version
```

## B2. Create the local broker configuration

From the repository root:

``` bash
cp config/kafka-vm3.properties.example config/kafka-vm3.properties
```

Check these important values:

``` properties
process.roles=broker
node.id=3
controller.quorum.voters=2@192.168.195.21:9093
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://192.168.195.167:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
```

Inspect the local storage directory:

``` bash
grep -E '^(process.roles|node.id|controller.quorum.voters|listeners|advertised.listeners|log.dirs)=' config/kafka-vm3.properties
```

Confirm: - `node.id=3` - `process.roles=broker` - Controller voter is
`2@192.168.195.21:9093` - Advertised broker address is
`192.168.195.167:9092` - `log.dirs` points to VM3's own storage
directory, not VM2's.

## B3. Wait for coordinated KRaft initialization

Do not format storage or start the broker independently. Coordinate with
Pranay on VM2 and follow Part A3. VM2 is the only controller in this
design, so the broker-failure demonstration should stop VM3, not VM2.

------------------------------------------------------------------------

# Part C --- VM4: Tadi (Consumer 1 + Spark Job 1 + Producer 2)

## C1. Install Python dependencies

From the repository root:

``` bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Verify the Python dependencies:

``` bash
python -c "import kafka, dotenv; print('Python dependencies OK')"
```

If the virtual environment cannot be created because `venv` is missing,
install `python3-venv` and retry. Use the repository's
`requirements.txt`; do not install an unrelated PySpark version with
`pip` just to resolve editor warnings.

## C2. Install or verify Spark 3.3.4

VM4 runs Spark Job 1, so it needs Apache Spark **3.3.4** and a
compatible Java/Python setup. The team's expected Spark installation
path is `/opt/spark`.

Verify:

``` bash
/opt/spark/bin/spark-submit --version
```

Configure environment variables for the current terminal. Adjust the
Python executable to the compatible version actually installed on VM4:

``` bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/opt/spark
export PATH="$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
```

The Python paths above are examples. Check the actual executable with
`which python3` and `python3 --version`; use the same compatible
interpreter for driver and worker. VM1's known working setup uses
`/usr/bin/python3.10`, but VM4 must be checked independently.

## C3. Create the environment file

Create a local `.env` file from the tracked example:

``` bash
cp config/.env.example .env
```

Edit `.env` and confirm these shared team settings:

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

Keep the remaining values from `.env.example`, including paths and
consumer group IDs. Check that the order consumer uses its intended
group ID and that the metrics consumer uses the metrics consumer group
ID. Do not commit `.env`.

## C4. Prepare local data directories

``` bash
mkdir -p data/landed data/processed
```

VM4's pipeline paths are: - Input landing file:
`data/landed/orders_landed.jsonl` - Spark Job 1 output:
`data/processed/order_metrics.jsonl`

Do not run the consumer, Spark job, or metrics producer until the team
has confirmed that the Kafka cluster and topics are healthy.

------------------------------------------------------------------------

# 4. Coordinated Kafka initialization (VM2 and VM3 only)

**Perform this section together, only after both broker configurations
are reviewed.** If either VM may already have initialized Kafka storage,
inspect it first. Do not reformat existing storage blindly.

On VM2, generate one cluster ID:

``` bash
/opt/kafka/bin/kafka-storage.sh random-uuid
```

Record the output and share the exact ID with the team.

If both brokers need initial formatting and their storage directories
are confirmed safe to initialize, format VM2:

``` bash
/opt/kafka/bin/kafka-storage.sh format \
  -t "<SHARED_CLUSTER_ID>" \
  -c config/kafka-vm2.properties
```

Then format VM3 using the **same** ID and its own config:

``` bash
/opt/kafka/bin/kafka-storage.sh format \
  -t "<SHARED_CLUSTER_ID>" \
  -c config/kafka-vm3.properties
```

Replace `<SHARED_CLUSTER_ID>` with the actual ID. Formatting changes
storage state and must not be repeated casually.

Start VM2 in its own terminal:

``` bash
./scripts/start_kafka_vm2.sh
```

Then start VM3 in its own terminal:

``` bash
./scripts/start_kafka_vm3.sh
```

Keep both broker terminals open. If either script reports an error, stop
and investigate; do not delete log directories as a quick fix.

## 5. Create and verify topics

Once both brokers are running and the cluster is confirmed healthy, use
Kafka client tools on a machine with access to VM2.

First list existing topics:

``` bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.195.21:9092 \
  --list
```

If `orders` does not exist, create it:

``` bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.195.21:9092 \
  --create \
  --topic orders \
  --partitions 2 \
  --replication-factor 2
```

If `order_metrics` does not exist, create it:

``` bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.195.21:9092 \
  --create \
  --topic order_metrics \
  --partitions 2 \
  --replication-factor 2
```

If a topic already exists, inspect it instead of deleting or recreating
it.

Describe each topic:

``` bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.195.21:9092 \
  --describe \
  --topic orders
```

Repeat for `order_metrics`. Verify that each has two partitions,
replication factor 2, and healthy ISR before running the pipeline.

## 6. Distributed pipeline run order

Only start this after the cluster and both topics have been verified.

1.  **VM4:** Start `python consumer/consume_orders.py`.
2.  **VM1:** Run `python producer/produce_orders.py`.
3.  Wait for publishing to finish, then stop the VM4 order consumer
    gracefully.
4.  **VM4:** Run
    `spark-submit --master 'local[*]' spark/job1_delivery_metrics.py`.
5.  **VM4:** Run `python producer/produce_metrics.py`.
6.  **VM1:** Start `python consumer/consume_metrics.py`.
7.  Once all metrics arrive, stop the VM1 metrics consumer gracefully.
8.  **VM1:** Run
    `spark-submit --master 'local[*]' spark/job2_operations_analytics.py`.
9.  Verify `restaurant_summary.csv` and `city_summary.csv`.

Activate `.venv` before running the Python producers/consumers on VM1 or
VM4. Spark jobs must use the configured Spark installation and
compatible Python interpreter.

## 7. Setup checklist

### VM2

-   [ ] Repository cloned and updated
-   [ ] ZeroTier connected; IP verified as `192.168.195.21`
-   [ ] Java installed and verified
-   [ ] Kafka 3.9.2 installed/verified
-   [ ] `config/kafka-vm2.properties` created and reviewed
-   [ ] Storage state checked; no formatting performed prematurely

### VM3

-   [ ] Repository cloned and updated
-   [ ] ZeroTier connected; IP verified as `192.168.195.167`
-   [ ] Java installed and verified
-   [ ] Kafka 3.9.2 installed/verified
-   [ ] `config/kafka-vm3.properties` created and reviewed
-   [ ] Storage state checked; no formatting performed prematurely

### VM4

-   [ ] Repository cloned and updated
-   [ ] ZeroTier connected; IP verified as `192.168.195.127`
-   [ ] Java and compatible Python installed
-   [ ] `.venv` created and `requirements.txt` installed
-   [ ] Spark 3.3.4 installed/verified
-   [ ] `.env` created locally and team values checked
-   [ ] `data/landed` and `data/processed` directories exist

### Team integration

-   [ ] VM2 and VM3 configurations reviewed together
-   [ ] Existing Kafka storage state checked on both brokers
-   [ ] One shared cluster ID coordinated
-   [ ] Brokers started and cluster health verified
-   [ ] Both topics verified at 2 partitions and replication factor 2
-   [ ] End-to-end pipeline tested
-   [ ] VM3 broker-failure demonstration recorded

## 8. Safety reminders

-   Do not commit `.env` or the actual `config/kafka-vm2.properties` /
    `config/kafka-vm3.properties` files.
-   Do not change or delete the original `order.jsonl`.
-   Do not copy Kafka storage between brokers.
-   Do not generate separate cluster IDs for VM2 and VM3.
-   Do not format Kafka storage if it may already be initialized.
-   Do not delete Kafka log directories to troubleshoot a connection
    problem.
-   Do not claim the pipeline is verified until the team has run it and
    recorded the results.
