---
title: Write Disposition in Ferry
---

# 🔀 Write Disposition in Ferry

**Write Disposition** determines how Ferry handles data when writing to a destination. It defines whether new data should be **replaced, appended, or merged** with existing data.

Ferry supports multiple write strategies to accommodate different data ingestion use cases.


## 📌 Supported Write Dispositions

| Disposition  | Behavior |
|-------------|----------|
| **replace** | Drops existing data in the destination and writes the new dataset. |
| **append**  | Adds new records without modifying existing data. |
| **merge**   | Merges incoming records with existing ones based on a primary key. |

---

## 🔄 Choosing the Right Write Disposition

- **Use `replace`** when you need a **fresh dataset every time** (e.g., full data refresh).  
- **Use `append`** when you want to **continuously add data** (e.g., log files, time-series data).  
- **Use `merge`** when you need **to consolidate updates** with existing data.  




