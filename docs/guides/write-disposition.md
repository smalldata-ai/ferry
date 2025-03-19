---
title: Write Disposition in Ferry
---

# 🔀 Write Disposition in Ferry

**Write Disposition** determines how Ferry handles data when writing to a destination. It defines whether new data should be **appended, replaced, or merged** with existing data.

Ferry supports multiple write strategies to accommodate different data ingestion use cases.


## 📌 Supported Write Dispositions

| Disposition  | Behavior |
|-------------|----------|
| **append**  | Adds new records without modifying existing data. |
| **replace** | Drops existing data in the destination and writes the new dataset. |
| **merge**   | Merges incoming records with existing ones based on a primary key. |

---

## 🔄 Choosing the Right Write Disposition

- **Use `append`** when you want to **continuously add data** (e.g., log files, time-series data).  
- **Use `replace`** when you need a **fresh dataset every time** (e.g., full data refresh).  
- **Use `merge`** when you need **to consolidate updates** with existing data.  

---

## 🛠️ Example: Setting Write Disposition in Ferry

You can specify the write disposition when making an ingestion request.

### **CLI Example**
```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source": "http_api",
    "destination": "postgres://user:password@host:port/database",
    "data": [
      {"id": 1, "name": "Alice", "amount": 100},
      {"id": 2, "name": "Bob", "amount": 200}
    ],
    "write_disposition": "merge"
  }'
