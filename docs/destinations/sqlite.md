---
title: Using SQLite as a Destination in Ferry
---

# 🗄️ Using SQLite as a Destination in Ferry

Ferry allows you to ingest data **into SQLite database**

## 📌 Prerequisites

Before using SQLite as a source, ensure:
- You have a valid SQLite database file (`.sqlite` or `.db`).

## `destination_uri` Format

To connect Ferry to an SQLite database, use the following connection string format:

```plaintext
  sqlite:///<path-to-database>.db
```  

### Parameters:
- **`<path-to-database>`** – Absolute or relative path to the SQLite database file (e.g., `/path/to/database.db`).

## `destination_table_name` Format

```plaintext
  users
```