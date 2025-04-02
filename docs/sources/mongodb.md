---
title: Using MongoDB as a Source in Ferry
---

# 🍃 Using MongoDB as a Source in Ferry

Ferry allows you to ingest data **from a MongoDB database** and move it to different destinations like **data warehouses, relational databases**.

## 📌 Prerequisites

Before using MongoDB as a source, ensure:
- MongoDB is running and accessible.
- You have a valid **connection string** (`mongodb://user:password@host:port/authDb?database=database`).
- The required **collection** exists in your MongoDB database.
- The user has **read access** to the database.

## `source_uri` Format

To connect Ferry to a MongoDB database, use the following connection string format:

```plaintext
  mongodb://<username>:<password>@<host>:<port>/<auth-db>?database=<database-name>
```

## `source_table_name` Format

```plaintext
  my_collection
```