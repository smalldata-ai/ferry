---
title: Using MySQL as a Destination in Ferry
---

# 🛢️ Using MySQL as a Destination in Ferry

Ferry allows you to ingest data **into a MySQL database** 

## 📌 Prerequisites

Before using MySQL as a source, ensure:
- MySQL is running and accessible.
- You have a valid **connection string** (`mysql://user:password@host:port/database`).
- The user has **write access** to the database.

## `destination_uri` Format

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

## `destination_table_name` Format

```plaintext
  my_schema.users
```