---
title: Using MotherDuck as a Destination in Ferry
---

# 🛢️ Using MotherDuck as a Destination in Ferry

Ferry allows you to ingest data **into a Destination database** from variety of source

## 📌 Prerequisites

Before using DuckDB as a destination, ensure:
- DuckDB is installed (`pip install duckdb` if using Python).
- You have a valid **database file** (`.duckdb`) or an **in-memory** database.


## `destination_uri` Format
To connect Ferry to a DuckDB database, use the following connection string format:

```plaintext
  duckdb:///<path-to-database>.duckdb
```

### Parameters:
- **`<path-to-database>`** – Path to the DuckDB database file (e.g., `/path/to/database.duckdb`).

## `destination_table_name` Format

```plaintext
  public.logs
```