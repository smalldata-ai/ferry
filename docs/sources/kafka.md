---
title: Using Kafka as a Source in Ferry  
---

# 📡 Using Kafka as a Source in Ferry

Ferry allows you to ingest data **from a Kafka topic** and move it to different destinations like **data warehouses, databases, or APIs**.

## 📌 Prerequisites

Before using Kafka as a source, ensure:
- Kafka and ZooKeeper are properly installed (e.g., via Confluent Platform).
- Kafka is running on your system (Linux/WSL recommended).
- You have a **topic** with data (optionally serialized with **Avro**, **JSON**, etc.).
- Schema Registry is running if you're using Avro serialization.

### 🖥️ Starting Kafka Services (Linux/WSL)

Run the following commands (adjust paths as needed based on your Kafka installation directory):

```bash
# Start ZooKeeper
/mnt/c/confluent-x.y.z/bin/zookeeper-server-start /mnt/c/confluent-x.y.z/etc/kafka/zookeeper.properties

# Start Kafka Broker
/mnt/c/confluent-x.y.z/bin/kafka-server-start /mnt/c/confluent-x.y.z/etc/kafka/server.properties

# Start Schema Registry (if using Avro)
# Make sure Kafka and ZooKeeper are running first
/mnt/c/confluent-x.y.z/bin/schema-registry-start /mnt/c/confluent-x.y.z/etc/schema-registry/schema-registry.properties
```

> 💡 Replace `x.y.z` with your actual Confluent version (e.g., `7.9.0`).

## `source_uri` Format

To connect Ferry to a Kafka source, use the following URI format:

```plaintext
kafka://<broker-address>?group_id=<group>&security_protocol=<protocol>&sasl_mechanisms=<mechanism>&sasl_username=<user>&sasl_password=<password>&schema_registry=<schema-registry-url>
```

### Parameters:
- **`<broker-address>`** – Kafka broker address (e.g., `localhost:9092`).
- **`group_id`** – Consumer group ID used for offset tracking.
- **`security_protocol`** – Security protocol (e.g., `PLAINTEXT`, `SASL_PLAINTEXT`, etc.).
- **`sasl_mechanisms`** – SASL mechanism (e.g., `PLAIN`, `SCRAM-SHA-256`).
- **`sasl_username`**, **`sasl_password`** – Optional, required for secured clusters.
- **`schema_registry`** – (Optional) URL to Schema Registry (e.g., `http://localhost:8081`) if using Avro.

## `source_table_name` Format

```plaintext
<topic-name>
```

### Example:

```plaintext
avro_topic
```

---

