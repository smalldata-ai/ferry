---
outline: deep
---

# 🚀 Getting Started with Ferry

Welcome to **Ferry ⛴️**, the lightweight and powerful data ingestion tool. Whether you're pulling data from a database, an API, or a file, **Ferry makes ingestion simple, fast, and observable**.

This guide will walk you through installation and your first data ingestion example.

## 🛠️ Installation

Ferry can be installed using **pip**:

```sh
pip install ferry
```

Verify the installation:
```sh
ferry --version
```

## ⚡ Your First Data Ingestion
Let's use cURL with Ferry’s HTTP API to ingest data. In this example, we'll transfer data from a `PostgreSQL` database to a `ClickHouse` data warehouse.

### Step 1: Start the Ferry Server
Run the Ferry service locally:

```sh
ferry serve
```

### Step 2: Send Data Using cURL
Use cURL to transfer data from a `PostgreSQL` database to a `ClickHouse` data warehouse.

```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": ""
    "source_uri": "postgresql://postgres:@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:@localhost:9000/dlt?http_port=8123&secure=0"
  }'
```

URI parameters:
- `identity`: a unique identifier for the pipeline
- `source_uri`: the source database uri
- `destination_uri`: the destination database uri

### Step 3: Check Data Transfer Status