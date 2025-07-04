---
title: Using PostgreSQL as a Destination in Ferry
---

# 🛢️ Using PostgreSQL as a Destination in Ferry

Ferry allows you to ingest data **into a Destination database** from variety of source

## 📌 Prerequisites

Before using PostgreSQL as a destination, ensure:
- PostgreSQL is running and accessible.
- You have a valid **connection string** (`postgres://user:password@host:port/database`).


## `destination_uri` Format
To connect Ferry to a PostgreSQL database, use the following connection string format:

```plaintext
  postgresql://<username>:<password>@<host>:<port>/<database-name>?sslmode=<sslmode>
```

### Parameters:
- **`<username>`** – PostgreSQL username.
- **`<password>`** – PostgreSQL password.
- **`<host>`** – Database host (e.g., `localhost` or `db.example.com`).
- **`<port>`** – PostgreSQL port (default: `5432`).
- **`<database-name>`** – Name of the database.
- **`<sslmode>`** – SSL mode (`disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`).

## `destination_table_name` Format

```plaintext
  public.users
```