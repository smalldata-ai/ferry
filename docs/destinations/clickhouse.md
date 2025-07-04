---
title: Using Clickhouse as a Destination in Ferry
---

# 🛢️ Using Clickhouse as a Destination in Ferry

Ferry allows you to ingest data **into a Destination database** from variety of source

## 📌 Prerequisites

Before using Clickhouse as a destination, ensure:
- Clickhouse is running and accessible.
- You have a valid **connection string** (`clickhouse://user:password@host:port/databasehttp_port=<http_port>&secure=<secure_toggle>`).


## `destination_uri` Format
To connect Ferry to a Clickhouse database, use the following connection string format:

```plaintext
  clickhouse://user:password@host:port/database?http_port=<http_port>&secure=<secure_toggle>
```

### Parameters:
- **`<user>`** – Clickhouse username.
- **`<password>`** – Clickhouse password.
- **`<host>`** – Database host (e.g., `localhost` or `db.example.com`).
- **`<port>`** – Clickhouse port (default: `9000`).
- **`<database-name>`** – Name of the database.
- **`<http_port>`** – HTTP Port - default is 8123
- **`<secure_toggle>`** – HTTP is ecure or not - 0 or 1

## `destination_table_name` Format

```plaintext
  public.transactions
```