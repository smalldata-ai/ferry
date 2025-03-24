# Merge
Merge Write Disposition updates existing records and inserts new ones based on a specified key or condition. 
It is useful for deduplicating data, handling upserts, and maintaining slowly changing dimensions. 
This mode ensures data consistency by intelligently combining new and existing records.

Ferry supports **three key merge strategies** that determine how records are updated when ingested into a destination.

## 📌 Supported Merge Strategies

| Strategy          | Behavior |
|------------------|----------|
| **delete-insert** | Deletes existing records that match incoming data based on a primary key, then inserts the new records. |
| **upsert**        | Updates existing records if a match is found; otherwise, inserts new records. |
| **scd2**         | Implements **Slowly Changing Dimensions Type 2 (SCD2)**, preserving historical versions of a record by creating new records with versioning. |


## ✨ Choosing the Right Merge Strategy

### **delete-insert** (Full Replacement of Matching Records)
- Deletes existing records in the destination table that match the **primary key** in the incoming data.
- Inserts new records after the deletion.
- Suitable for cases where **stale records need to be fully replaced**.

**Use Case:**  
✔️Ideal for **batch processing** where the latest dataset should fully replace old data.

### **upsert** (Update Existing, Insert New)
- If a record with a **matching primary key** exists, it is **updated**.  
- If no match is found, a **new record is inserted**.  
- Ensures that only **new or modified** records are updated.

**Use Case:**  
✔️ Best for **incremental data ingestion** without losing existing information.  

### **scd2** (Slowly Changing Dimensions Type 2)
- Instead of modifying existing records, **a new version of the record is created** with a timestamp or versioning column.  
- Allows **historical tracking of record changes** over time.  
- Ensures that old records remain untouched while **storing changes as new rows**.

**Use Case:**  
✔️ Ideal for **tracking changes in customer or product data** over time without overwriting previous versions.  

