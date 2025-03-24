---
title: Using MariaDB as a Source in Ferry
---

# 🛢️ Using MariaDB as a Source in Ferry

Ferry allows you to ingest data **from a MariaDB database** and move it to different destinations like **data warehouses, APIs, or other databases**.

## 📌 Prerequisites

Before using MariaDB as a source, ensure:
- MariaDB is running and accessible.
- You have a valid **connection string** (`mariadb://user:password@host:port/database`).
- The required table exists in your MariaDB database.
- The user has **read access** to the database.

## `source_uri` Format

To connect Ferry to a MariaDB database, use the following connection string format:

```plaintext
  mariadb://<username>:<password>@<host>:<port>/<database-name>
```  

### Parameters:
- **`<username>`** – PostgreSQL username.
- **`<password>`** – PostgreSQL password.
- **`<host>`** – Database host (e.g., `localhost` or `db.example.com`).
- **`<port>`** – PostgreSQL port (default: `5432`).
- **`<database-name>`** – Name of the database.

## `source_table_name` Format

```plaintext
  my_schema.table_name
```