# /ingest
The ingest api is the core api to move data between source and destination.


## Example
This is a simple api request to move data between `PostgreSQL` database to a `ClickHouse` data warehouse.
```json
{
  "identity": "string",
  "source_uri": "string",
  "destination_uri": "string",
  "dataset_name": "string",
  "resources": [
    {
      "source_table_name": "string",
      "destination_table_name": "string",
      "incremental_config": {
        "incremental_key": "string",
        "start_position": 0,
        "end_position": 0,
        "lag_window": 0,
        "boundary_mode": "start-end"
      },
      "write_disposition": "replace",
      "replace_config": {
        "strategy": "truncate-and-insert"
      },
      "merge_config": {
        "strategy": "delete-insert",
        "delete_insert_config": {
          "primary_key": "string",
          "merge_key": "string",
          "hard_delete_column": "string",
          "dedup_sort_column": {
            "additionalProp1": "asc",
            "additionalProp2": "asc",
            "additionalProp3": "asc"
          }
        },
        "scd2_config": {
          "natural_merge_key": "string",
          "partition_merge_key": "string",
          "validity_column_names": [
            "valid_from",
            "valid_to"
          ],
          "active_record_timestamp": "9999-12-31",
          "use_boundary_timestamp": false
        },
        "upsert_config": {
          "primary_key": "string",
          "hard_delete_column": "string"
        }
      }
    }
  ]
}
```

## Parameters Descriptions


### **Top-Level Fields**
| Field                 | Type     | Required | Description |
|----------------------|---------|----------|-------------|
| **`identity`**       | string  | ✅ Yes   | A unique identifier for the ingestion job. |
| **`source_uri`**     | string  | ✅ Yes   | The URI of the data source (e.g., database, file storage). |
| **`destination_uri`**| string  | ✅ Yes   | The URI where data should be ingested. |
| **`dataset_name`**   | string  | ❌ No   | The dataset where the data will be stored. |
| **`resources`**      | array   | ✅ Yes   | A list of resources & the ingestion configuration. |

---

### **Resources Array**
Each item in the `resources` array defines how a specific resources should be ingested.

#### **Table Mapping**
| Field                    | Type    | Required | Description |
|--------------------------|---------|----------|-------------|
| **`source_table_name`**  | string  | ✅ Yes   | Name of the source table. |
| **`destination_table_name`** | string  | ❌ No   | Name of the destination table. |

---

#### **Incremental Configuration (`incremental_config`)** *(Optional)*
| Field                  | Type    | Required | Description |
|------------------------|---------|----------|-------------|
| **`incremental_key`**  | string  | ✅ Yes   | The column used for incremental loading. |
| **`start_position`**   | integer | ❌ No   | The starting point for incremental extraction. |
| **`end_position`**     | integer | ❌ No   | The endpoint for incremental extraction. |
| **`lag_window`**       | integer | ❌ No   | A buffer to ensure data consistency. |
| **`boundary_mode`**    | string  | ❌ No   | Defines the mode (`start-end`, etc.). |

---

#### **Write Disposition (`write_disposition`)** *(Optional)* *(default: replace)*
Determines how data is written:
| Field                | Type    | Required | Description |
|----------------------|---------|----------|-------------|
| **`write_disposition`** | string | ✅ Yes  | Strategy for writing data (`replace`, `append`, `merge`). |

#### **Replace Configuration (`replace_config`)** *(Mandatory when `write_disposition` is `replace`)*
| Field        | Type    | Required | Description |
|-------------|---------|----------|-------------|
| **`strategy`** | string | ✅ Yes | Defines how replacement is handled (e.g., `truncate-and-insert`,`insert-from-staging`,`staging-optimized`). |

---

#### **Merge Configuration (`merge_config`)** *(Mandatory when `write_disposition` is `merge`)*
Defines different merge strategies:
| Field        | Type    | Required | Description |
|-------------|---------|----------|-------------|
| **`strategy`** | string | ✅ Yes | Merge method (e.g., `delete-insert`,`scd2`,`upsert`). |

##### **Delete-Insert Configuration (`delete_insert_config`)** *(Mandatory when `merg_config.strategy` is `delete-insert`)*
| Field                | Type    | Required | Description |
|----------------------|---------|----------|-------------|
| **`primary_key`**   | string  | ✅ Yes   | Primary key for identifying records. |
| **`merge_key`**     | string  | ✅ Yes   | Key used for merging. |
| **`hard_delete_column`** | string  | ❌ No  | Column used to mark hard deletions. |
| **`dedup_sort_column`**  | object  | ❌ No  | Specifies sorting order for deduplication. |

---

##### **Slowly Changing Dimension Type 2 (`scd2_config`)** *(Mandatory when `merg_config.strategy` is `scd2`)*
| Field                     | Type    | Required | Description |
|---------------------------|---------|----------|-------------|
| **`natural_merge_key`**   | string  | ✅ Yes   | Key used for merging historical data. |
| **`partition_merge_key`** | string  | ✅ Yes   | Partition key for merges. |
| **`validity_column_names`** | array | ✅ Yes   | Column names for validity range. |
| **`active_record_timestamp`** | string | ❌ No | Default timestamp for active records. |
| **`use_boundary_timestamp`** | boolean | ❌ No | Whether to use boundary timestamps. |

---

##### **Upsert Configuration (`upsert_config`)** *(Mandatory when `merg_config.strategy` is `upsert`)*
| Field                  | Type    | Required | Description |
|------------------------|---------|----------|-------------|
| **`primary_key`**      | string  | ✅ Yes   | Key for identifying records. |
| **`hard_delete_column`** | string  | ❌ No  | Column marking records for deletion. |