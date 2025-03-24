# Ferry ⛴️

*A lightweight and scalable data ingestion framework*
## Overview
Ferry is an open-source data ingestion framework designed to simplify data movement between sources and destinations. 
It supports multiple interfaces, including REST APIs, CLI, gRPC making it suitable for diverse data engineering workflows.

## Features
- **Multi-Protocol Support**: Supports command-line execution, REST API, and gRPC for flexible integration.
- **Scalability**: Handles large-scale data transfers efficiently.
- **Monitoring & Observability**: Tracks records transferred, volume, and job status.
- **Incremental Loading**: Ingest only new or changed data with efficient checkpointing to avoid redundant processing.
- **Merge Strategies for Seamless Updates**: Supports append, upsert, and merge strategies for handling real-time and batch data ingestion.

## Installation
```sh
pip install ferry
```

## Quick Start
```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:password@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:password@localhost:9000/db_name?http_port=8123&secure=0",
    "resources": [
      {"source_table_name": "users"}
    ]
  }'
```
## Documentation
You can view the full documentation [here](https://smalldata-ai.github.io/ferry/guides/getting-started.html).

## 📥 Supported Sources & Destinations

| System                 | Source ✅ | Destination ✅ |
|------------------------|:--------:|:-------------:|
| **PostgreSQL** (`postgres`) | ✅ | ✅ |
| **DuckDb** (`duckdb`) | ✅ | ✅ |
| **Amazon S3** (`s3`) | ✅ | ✅ |
| **MySQL** (`mysql`) | ✅ | ✅ |
| **ClickHouse** (`clickhouse`) | ✅ | ✅ |
| **Google Cloud Storage** (`gcs`) | ✅ | ✅ |
| **Local Files** (`file`, CSV, JSON, Parquet) | ✅ | ✅ |
| **Snowflake** (`gcs`) | ✅ | ✅ |
| **Mongodb** (`gcs`) | ✅ | ❌|
| **BigQuery** (`bigquery`) | ❌ | ✅ |

🔹 **Incremental loading** is available for **PostgreSQL, MySQL, ClickHouse, and file-based sources**.  
🔹 **Merge strategies (`upsert`, `delete-insert`, `scd2`) are supported for SQL-based destinations** (PostgreSQL, MySQL, ClickHouse, BigQuery).  

## Acknowledgements
