# upsert
The **Upsert Strategy** is used to efficiently update existing records while inserting new ones in a database or data warehouse. It ensures that incoming data is merged with existing data based on a unique key, preventing duplicates and maintaining data integrity.

## How It Works
- **Check for Existing Records:** Match incoming data against the existing table using a unique key (e.g., `ID`, `order_number`).
- **Update Matching Records:** If a record exists, update the relevant fields with new values.
- **Insert New Records:** If no matching record is found, insert the new row into the table.
- **Ensure Data Consistency:** This process maintains a single source of truth, reducing redundancy.

## When to Use It
- When handling **incremental updates** where some records need modification, while others are new.
- In **event-driven architectures**, where real-time updates are needed in a data pipeline.

## Example
```js
{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:@localhost:9000/dlt?http_port=8123&secure=0",
    "resources": [
      {
        "source_table_name": "users",
        "write_disposition_config": { // [!code focus]
          "type": "merge" // [!code focus]
          "strategy": "upsert" // [!code focus]
          "config": { // [!code focus]
                "primary_key": "id", // [!code focus]
                "hard_delete_column": "deleted_at", // [!code focus]
            } // [!code focus]
        } // [!code focus]
      }
    ]
}
```

## Parameters Table

| Parameter            | Type                                      | Required | Description |
|----------------------|-----------------------------------------|----------|-------------|
| `primary_key`       | `string/list` | ✅ Yes  | The primary key(s) is used for primary-key based upserts. |
| `hard_delete_column` | `string`                         | ❌ No  | A column that marks records for deletion from the destination dataset. |

## Parameter Details

### 1. `primary_key`
- Used to uniquely identify records in the dataset.
- Can be a **single column** (string) or **multiple columns** (tuple).
- Update a record if the key exists in the target table
- Insert a record if the key does not exist in the target table
- Needs to be unique

### 2. `hard_delete_column`
- Specifies a column that indicates whether a record should be deleted.
- If this column is present and marked (e.g., `is_deleted = True`), the corresponding record will be removed before inserting new data.

> [!WARNING]
> The primary-key is expected to be unique.De-duplication is not handled in this strategy.