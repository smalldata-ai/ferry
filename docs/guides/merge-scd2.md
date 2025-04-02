# scd2
**Slowly Changing Dimension Type 2 (SCD2)** is a data management strategy used to track historical changes in dimensional tables. Instead of updating existing records, SCD2 maintains a full history by inserting new versions of records with timestamps or versioning fields.

## How It Works
- **Identify Changes:** Compare incoming data with the existing table based on a unique key (e.g., Customer ID, Product ID).
- **Insert New Record for Changes:** When a change is detected, insert a new row with updated values while keeping the old row intact.
- **Manage Effective Dates:** Use fields such as `effective_start_date` and `effective_end_date` to track active and historical records.


## When to Use It
- When **historical data tracking is required**, and changes need to be preserved.
- For **dimension tables** in a data warehouse, where attributes evolve over time (e.g., customer address changes).
- To support **time-travel queries**, enabling analysis of past data states.

## Example
```js
{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:@localhost:9000/dlt?http_port=8123&secure=0",
    "resources": [
      {
        "source_table_name": "users",
        "write_disposition": "merge",
        "merge_config": { // [!code focus]
            "strategy": "scd2", // [!code focus]
            "scd2_config": { // [!code focus]
                "natural_merge_key": "id", // [!code focus]
                "partition_merge_key": "oo", // [!code focus]
                "validity_column_names": "oo", // [!code focus]
                "active_record_timestamp": "oo", // [!code focus]
                "use_boundary_timestamp": "oo", // [!code focus]
            } // [!code focus]
        } // [!code focus]
      }
    ]
}
```

## Parameters Table

| Parameter                 | Type                                      | Required | Description |
|---------------------------|-------------------------------------------|----------|-------------|
| `natural_merge_key`       | `string/list`  | ❌ No  | Key(s) that define unique records. Prevents absent rows from being retired in incremental loads. |
| `partition_merge_key`     | `string/list`  | ❌ No  | Key(s) defining partitions. Retires only absent records within loaded partitions. |
| `validity_column_names`   | `list`                              | ✅ Yes  | Column names for validity periods, typically used to track historical changes (default: `["valid_from", "valid_to"]`). |
| `active_record_timestamp` | `string`                                    | ✅ Yes  | Timestamp value representing active records (default: `"9999-12-31"`). |
| `use_boundary_timestamp`  | `bool`                                   | ✅ Yes  | Determines if record validity windows should include a boundary timestamp (default: `False`). |

## Parameter Details

### 1. `natural_merge_key`
- Defines the **natural key(s)** that uniquely identify records.
- Ensures that records absent from the current batch are **not automatically retired** during incremental loads.
- Can be a **single column** (string) or **multiple columns** (tuple).

### 2. `partition_merge_key`
- Defines the **partition key(s)** for merging data.
- Ensures only **absent records within the loaded partitions** are retired.
- Helps optimize updates by limiting the scope of changes to specific partitions.

### 3. `validity_column_names`
- A list of column names used to track **validity periods** for historical records.
- Default columns: `["valid_from", "valid_to"]`.
- Used to mark when a record was active and when it was replaced.

### 4. `active_record_timestamp`
- The **timestamp value** used to indicate an **active record**.
- Default value: `"9999-12-31"`, representing an open-ended validity period.
- Helps identify the latest record for an entity.

### 5. `use_boundary_timestamp`
- Determines whether **record validity windows** should include a **boundary timestamp**.
- If `True`, the validity range will explicitly track timestamp boundaries.
- If `False`, standard validity period logic is applied.