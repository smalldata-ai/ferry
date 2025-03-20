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

## Strategies
### delete-insert
The **delete-insert Strategy** is a data update approach where outdated records are removed before new data is inserted. This method ensures data consistency, especially in scenarios where records need to be fully replaced rather than updated incrementally.

#### How It Works
- **Delete Existing Data:** Remove records from the target table that match a specific condition (e.g., based on a time window or primary key).
- **Insert New Data:** Load fresh data from the source into the target table.
- **Ensure Consistency:** By removing old data before inserting new entries, this strategy avoids duplicates and maintains an accurate dataset.

#### When to Use It
- When dealing with **immutable source data**, where updates are not tracked.
- For **incremental refresh scenarios**, such as daily or hourly data updates.
- When **source records are replaced rather than modified**, ensuring historical accuracy.

### scd2
**Slowly Changing Dimension Type 2 (SCD2)** is a data management strategy used to track historical changes in dimensional tables. Instead of updating existing records, SCD2 maintains a full history by inserting new versions of records with timestamps or versioning fields.

#### How It Works
- **Identify Changes:** Compare incoming data with the existing table based on a unique key (e.g., Customer ID, Product ID).
- **Insert New Record for Changes:** When a change is detected, insert a new row with updated values while keeping the old row intact.
- **Manage Effective Dates:** Use fields such as `effective_start_date` and `effective_end_date` to track active and historical records.


#### When to Use It
- When **historical data tracking is required**, and changes need to be preserved.
- For **dimension tables** in a data warehouse, where attributes evolve over time (e.g., customer address changes).
- To support **time-travel queries**, enabling analysis of past data states.

### upsert
The **Upsert Strategy** is used to efficiently update existing records while inserting new ones in a database or data warehouse. It ensures that incoming data is merged with existing data based on a unique key, preventing duplicates and maintaining data integrity.

#### How It Works
- **Check for Existing Records:** Match incoming data against the existing table using a unique key (e.g., `ID`, `order_number`).
- **Update Matching Records:** If a record exists, update the relevant fields with new values.
- **Insert New Records:** If no matching record is found, insert the new row into the table.
- **Ensure Data Consistency:** This process maintains a single source of truth, reducing redundancy.

#### When to Use It
- When handling **incremental updates** where some records need modification, while others are new.
- In **event-driven architectures**, where real-time updates are needed in a data pipeline.
