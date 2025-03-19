---
title: Using PostgreSQL as a Source in Ferry
---

# 🛢️ Using PostgreSQL as a Source in Ferry

Ferry allows you to ingest data **from a PostgreSQL database** and move it to different destinations like **data warehouses, APIs, or other databases**. 

With built-in **incremental loading, merge strategies, and observability**, Ferry makes it easy to extract and transfer data from PostgreSQL efficiently.

---

## 📌 Prerequisites

Before using PostgreSQL as a source, ensure:
- PostgreSQL is running and accessible.
- You have a valid **connection string** (`postgres://user:password@host:port/database`).
- The required table exists in your PostgreSQL database.

---

## 🛠️ Ingesting Data from PostgreSQL

You can extract data from a PostgreSQL database using **Ferry’s CLI** or **REST API**.

### **CLI Example**
```sh
ferry ingest postgres://user:password@host:port/database \
  --source-table orders \
  --destination warehouse
