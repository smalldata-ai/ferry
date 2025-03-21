---
title: Using MySQL as a Source in Ferry
---

# 🛢️ Using MySQL as a Source in Ferry

Ferry allows you to ingest data **from a MySQL database** and move it to different destinations like **data warehouses, APIs, or other databases**.

## 📌 Prerequisites

Before using MySQL as a source, ensure:
- MySQL is running and accessible.
- You have a valid **connection string** (`mysql://user:password@host:port/database`).
- The required table exists in your MySQL database.
- The user has **read access** to the database.

## `source_uri` Format

To connect Ferry to a MySQL database, use the following connection string format:

```plaintext
  mysql://<username>:<password>@<host>:<port>/<database-name>
```  

### Parameters:
- **`<username>`** – PostgreSQL username.
- **`<password>`** – PostgreSQL password.
- **`<host>`** – Database host (e.g., `localhost` or `db.example.com`).
- **`<port>`** – PostgreSQL port (default: `5432`).
- **`<database-name>`** – Name of the database.

## `source_table_name` Format

```plaintext
  my_schema.users
```