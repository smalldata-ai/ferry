# delete-insert
The **delete-insert Strategy** is a data update approach where outdated records are removed before new data is inserted. This method ensures data consistency, especially in scenarios where records need to be fully replaced rather than updated incrementally.

## How It Works
- **Delete Existing Data:** Remove records from the target table that match a specific condition (e.g., based on a primary key).
- **Insert New Data:** Load fresh data from the source into the target table.
- **Ensure Consistency:** By removing old data before inserting new entries, this strategy avoids duplicates and maintains an accurate dataset.

## When to Use It
- When dealing with **immutable source data**, where updates are not tracked.
- For **incremental refresh scenarios**, such as daily or hourly data updates.
- When **source records are replaced rather than modified**, ensuring historical accuracy.


## Example
```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:@localhost:9000/dlt?http_port=8123&secure=0",
    "resources": [
      {
        "source_table_name": "users",
        "write_disposition": "merge",
        "merge_config": {
            "strategy": "delete-insert",
            "delete_insert_config": {
                "primary_key": "id",
                "merge_key": "user_uid",
                "hard_delete_column": "deleted_at",
                "dedup_sort_column": {"registered_at": "asc"}
            }
        }
      }
    ]
  }'
```
## Parameters Table

| Parameter            | Type                                      | Required | Description |
|----------------------|-----------------------------------------|----------|-------------|
| `primary_key`       | `string/list` | ❌ No  | The primary key(s) used to identify unique records for deletion before insert. |
| `merge_key`        | `string/list` | ❌ No  | The merge key(s) used to match records between the source and destination. |
| `hard_delete_column` | `string`                         | ❌ No  | A column that marks records for deletion from the destination dataset. |
| `dedup_sort_column` | `map`        | ❌ No  | A column used to sort records before deduplication, following the specified sorting order. |

## Parameter Details

### 1. `primary_key`
- Used to uniquely identify records in the dataset.
- Can be a **single column** (string) or **multiple columns** (tuple).
- Helps ensure that deleted records match exactly before inserting new data.

### 2. `merge_key`
- Defines the columns used for merging data from the source and destination.
- Can be a **single column** or **multiple columns** to support complex merge scenarios.

### 3. `hard_delete_column`
- Specifies a column that indicates whether a record should be deleted.
- If this column is present and marked (e.g., `is_deleted = True`), the corresponding record will be removed before inserting new data.

### 4. `dedup_sort_column`
- A dictionary defining sorting rules for deduplication.
- The key represents the column name, and the value (`SortOrder.ASC` or `SortOrder.DESC`) determines sorting order.
- Useful for keeping the latest or most relevant records before inserting new data.

> [!WARNING]
> If the merge write disposition is used without specifying merge or primary keys, it will default to append mode. In this case, the new data will be inserted from a staging table as a single transaction for most destinations.
