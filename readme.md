# Ferry

![Ferry](Ferry ⛴️)  
*A lightweight and scalable data ingestion framework*

## Overview
Ferry is an open-source data ingestion framework designed to simplify data movement between sources and destinations. It supports multiple interfaces, including REST APIs, CLI, gRPC making it suitable for diverse data engineering workflows.

## Features
- **Multi-Protocol Support**: Supports command-line execution, REST API, and gRPC for flexible integration.
- **Scalability**: Handles large-scale data transfers efficiently.
- **Monitoring & Observability**: Tracks records transferred, volume, and job status.
- **Extensibility**: Allows custom connectors for new data sources and destinations.

## Installation
```sh
pip install ferry
```

## Quick Start

### 1. Use the REST API
```sh
curl -X POST "http://localhost:8000/ingest" -H "Content-Type: application/json" -d '{ "source": "postgresql", "destination": "s3", "config": "config.yaml" }'
```


## Supported Sources & Destinations
- **Sources**: PostgreSQL, MySQL, Kafka, CSV, JSON, etc.
- **Destinations**: S3, BigQuery, Snowflake, ClickHouse, etc.
