---
title: Merge Strategy in Ferry
---

# 🔀 Merge Strategy in Ferry

**Merge Strategy** in Ferry defines how new data is integrated into an existing dataset. Inspired by **dlthub's merge strategy**, Ferry provides flexible approaches to efficiently manage **incremental updates, historical tracking, and record deduplication**.

Ferry supports **three key merge strategies** that determine how records are updated when ingested into a destination.

---

## 📌 Supported Merge Strategies

| Strategy          | Behavior |
|------------------|----------|
| **delete-insert** | Deletes existing records that match incoming data based on a primary key, then inserts the new records. |
| **upsert**        | Updates existing records if a match is found; otherwise, inserts new records. |
| **scd2**         | Implements **Slowly Changing Dimensions Type 2 (SCD2)**, preserving historical versions of a record by creating new records with versioning. |

---

## 🔄 Choosing the Right Merge Strategy

### 1️⃣ **delete-insert** (Full Replacement of Matching Records)
- Deletes existing records in the destination table that match the **primary key** in the incoming data.
- Inserts new records after the deletion.
- Suitable for cases where **stale records need to be fully replaced**.

**Use Case:**  
✅ Ideal for **batch processing** where the latest dataset should fully replace old data.

### 2️⃣ **upsert** (Update Existing, Insert New)
- If a record with a **matching primary key** exists, it is **updated**.  
- If no match is found, a **new record is inserted**.  
- Ensures that only **new or modified** records are updated.

**Use Case:**  
✅ Best for **incremental data ingestion** without losing existing information.  

### 3️⃣ **scd2** (Slowly Changing Dimensions Type 2)
- Instead of modifying existing records, **a new version of the record is created** with a timestamp or versioning column.  
- Allows **historical tracking of record changes** over time.  
- Ensures that old records remain untouched while **storing changes as new rows**.

**Use Case:**  
✅ Ideal for **tracking changes in customer or product data** over time without overwriting previous versions.  

---

## 🛠️ Example: Setting Merge Strategy in Ferry

You can define the merge strategy when making an ingestion request.

curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source": "http_api",
    "destination": "postgres://user:password@host:port/database",
    "data": [
      {"id": 1, "name": "Alice", "amount": 100},
      {"id": 2, "name": "Bob", "amount": 200}
    ],
    "merge_strategy": "upsert"
  }'


## 🚀 Why Use Merge Strategies?
 - Optimized for incremental data ingestion – minimizes unnecessary updates.
 - Prevents data loss and duplication – ensures only relevant updates are applied.
 - Supports historical tracking – scd2 allows for full history retention.
 - Efficient and scalable – works well for batch and streaming ingestion.