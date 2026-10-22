# VM3 Setup --- Hemaksh (Kafka Broker 2)

This guide prepares VM3 for the Distributed Food Ordering System. VM3
runs Kafka Broker 2. VM2 hosts the only KRaft controller, so VM3 must
use VM2's controller quorum.

## 1. VM3 details

-   Team member: Hemaksh
-   ZeroTier IP: `192.168.195.167`
-   Role: Kafka Broker 2
-   Kafka version: Apache Kafka 3.9.2
-   Expected Kafka installation: `/opt/kafka`

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

Check that the configuration template and startup script exist:

``` bash
ls config/kafka-vm3.properties.example
ls scripts/start_kafka_vm3.sh
```

## 3. Install prerequisites

``` bash
sudo apt update
sudo apt install -y git python3 openjdk-11-jdk
```

Verify:

``` bash
git --version
python3 --version
java -version
```

Install ZeroTier if necessary, join the team's existing network, and ask
the network administrator to authorize the VM. Confirm the address:

``` bash
ip -br addr
```

Verify that the VM is using `192.168.195.167` as its ZeroTier address.

## 4. Install or verify Kafka 3.9.2

Use the same Kafka version as VM2. If Kafka is already installed, do not
reinstall over it without checking the current installation.

``` bash
/opt/kafka/bin/kafka-topics.sh --version
```

If Kafka is installed elsewhere, coordinate the path change with the
team and update the startup script consistently.

## 5. Create the local broker configuration

From the repository root:

``` bash
cp config/kafka-vm3.properties.example config/kafka-vm3.properties
```

Check the important properties:

``` bash
grep -E '^(process.roles|node.id|controller.quorum.voters|listeners|advertised.listeners|log.dirs)=' config/kafka-vm3.properties
```

The expected settings include:

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

Check that `log.dirs` points to VM3's own local Kafka storage directory.
Do not use VM2's storage directory.

The local `config/kafka-vm3.properties` file should remain untracked by
Git; commit the `.example` template instead.

## 6. Important: wait for coordinated KRaft initialization

**Do not format Kafka storage or start the broker independently.**

Coordinate with the VM2 owner before initializing the cluster. The team
must first confirm the storage state on both brokers and generate one
shared cluster ID. If formatting is required, VM3 must use that same
cluster ID and its own configuration file.

Never generate a separate cluster ID for VM3. Do not reformat existing
initialized storage without inspecting it and agreeing on the plan.

When the team confirms that initialization is complete and VM2 is
running, start VM3 from the repository root:

``` bash
./scripts/start_kafka_vm3.sh
```

Keep the terminal open. If startup fails, capture the error and
investigate it rather than deleting Kafka log directories.

## 7. After the cluster starts

From a machine with Kafka client tools, inspect the cluster and topics
using VM2 as the bootstrap server:

``` bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.195.21:9092 \
  --list
```

Describe both topics:

``` bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.195.21:9092 \
  --describe \
  --topic orders
```

``` bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.195.21:9092 \
  --describe \
  --topic order_metrics
```

The intended configuration is two partitions and replication factor 2
for each topic. Verify actual replicas and ISR before declaring the
cluster healthy.

## 8. VM3 checklist

-   [ ] Repository cloned or updated
-   [ ] Java installed and verified
-   [ ] ZeroTier connected and IP confirmed as `192.168.195.167`
-   [ ] Kafka 3.9.2 installed or verified
-   [ ] `config/kafka-vm3.properties` created and reviewed
-   [ ] `log.dirs` points to VM3's own storage
-   [ ] Existing storage state checked
-   [ ] KRaft initialization coordinated with VM2
-   [ ] Broker started only after the team confirms initialization
-   [ ] Topics and ISR verified

## Safety reminders

-   Do not format Kafka storage until the team has reviewed its existing
    state.
-   Do not copy VM2's Kafka data directory to VM3.
-   Do not generate a separate cluster ID.
-   Do not commit `config/kafka-vm3.properties`.
-   The broker-failure demonstration should stop VM3, not VM2, because
    VM2 hosts the only controller.
