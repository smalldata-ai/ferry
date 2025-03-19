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
<br><br>
Verify the installation:
```sh
ferry --version
```

## Your First Data Ingestion
Let's use cURL with Ferry’s REST API to ingest data. In this example, we'll transfer data from a PostgreSQL database to a ClickHouse database.

### Step 1: Start the Ferry Server
Run the Ferry service locally:

```sh
ferry serve
```

### Step 2: Send Data Using cURL
Use cURL to transfer data from a PostgreSQL database to a ClickHouse database.

```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source": "http_api",
    "destination": "postgres://user:password@host:port/database",
    "data": [
      {"id": 1, "name": "Alice", "amount": 100},
      {"id": 2, "name": "Bob", "amount": 200}
    ],
    "merge_strategy": "upsert"
  }'

```

Explanation of Parameters
source – Defines where the data is coming from (e.g., http_api, csv, database).
destination – The target where the data will be stored (e.g., PostgreSQL, Data Warehouse).
data – A JSON payload containing the records to be ingested.
merge_strategy – Defines how data should be merged (append, upsert, replace).