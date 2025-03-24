---
title: Using SQLite as a Source in Ferry
---

# 🗄️ Using SQLite as a Source in Ferry

Ferry allows you to ingest data **from an SQLite database** and move it to different destinations like **data warehouses, APIs, or other databases**.

## 📌 Prerequisites

Before using SQLite as a source, ensure:
- You have a valid SQLite database file (`.sqlite` or `.db`).
- The required table exists in your SQLite database.
- Ferry has **read access** to the database file.

## `source_uri` Format

To connect Ferry to an SQLite database, use the following connection string format:

```plaintext
  sqlite:///<path-to-database>.db
```  

### Parameters:
- **`<path-to-database>`** – Absolute or relative path to the SQLite database file (e.g., `/path/to/database.db`).

## `source_table_name` Format

```plaintext
  users
```