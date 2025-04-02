---
title: Using DuckDB as a Source in Ferry
---

# 🦆 Using DuckDB as a Source in Ferry

Ferry allows you to ingest data **from a DuckDB database** and move it to different destinations like **data warehouses, APIs, or other databases**.

## 📌 Prerequisites

Before using DuckDB as a source, ensure:
- DuckDB is installed (`pip install duckdb` if using Python).
- You have a valid **database file** (`.duckdb`) or an **in-memory** database.
- The required tables exist in your DuckDB database.

## `source_uri` Format
To connect Ferry to a DuckDB database, use the following connection string format:

```plaintext
  duckdb:///<path-to-database>.duckdb
```

### Parameters:
- **`<path-to-database>`** – Path to the DuckDB database file (e.g., `/path/to/database.duckdb`).

## `source_table_name` Format

```plaintext
  main.users
```