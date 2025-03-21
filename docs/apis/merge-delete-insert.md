# /ferry
`write_disposition`=`merge` && `strategy`=`delete-insert`
<br>
Merges new data into the destination using merge_key and deduplicates new data using primary_key
[Refer here](/guides/merge-delete-insert)

## Request Payload
```json
{
  "identity": "string",
  "source_uri": "string",
  "destination_uri": "string",
  "resources": [
    {
      "source_table_name": "string",
      "write_disposition": "merge",
      "merge_config": {
        "strategy": "delete-insert",
        "delete_insert_config": {
          "primary_key": "string",
          "merge_key": "string",
          "hard_delete_column": "string",
          "dedup_sort_column": {
            "additionalProp1": "asc",
            "additionalProp2": "asc",
          }
        }
      }
    }
  ]
}
```

## Parameters Descriptions

#### **Write Disposition (`write_disposition`)** *(Optional)* *(default: replace)*
Determines how data is written:
| Field                | Type    | Required | Description |
|----------------------|---------|----------|-------------|
| **`write_disposition`** | string | ✅ Yes  | Strategy for writing data (`replace`, `append`, `merge`). |

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
