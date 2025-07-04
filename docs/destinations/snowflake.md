---
title: Using Snowflake as a Destination in Ferry
---

# 🛢️ Using Snowflake as a Destination in Ferry

Ferry allows you to ingest data **into a Destination database** from variety of source

## 📌 Prerequisites

Before using Snowflake as a destination, ensure:
- Snowflake is running and accessible.
- You have a valid **connection string** (`snowflake://user:password@account/dbName/dataset`).


## `destination_uri` Format
To connect Ferry to a Snowflake database, use the following connection string format:

```plaintext
  snowflake://user:password@account/dbName/dataset
```

### Parameters:
- **`<user>`** – Snowflake username.
- **`<password>`** – Snowflake password.
- **`<account>`** – Snowflake Account identifier.
- **`<dbName>`** – Snowflake Database Name.
- **`<dataset>`** – Snowflake Dataset.

## `destination_table_name` Format

```plaintext
  public.transactions
```