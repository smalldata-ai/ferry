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
      "write_disposition_config": { 
          "type": "merge", 
          "strategy": "delete-insert",
          "config": {
                "primary_key": "string",
                "merge_key": "string",
                "hard_delete_column": "string",
                "dedup_sort_column": {
                  "additionalProp1": "asc",
                  "additionalProp1": "asc"
                }
            }
        }
    }
  ]
}
```

## Parameters Descriptions

#### **Write Disposition (`write_disposition_config`)** *(Optional)* *(default: replace)*
Determines how data is written:
| Field                | Type    | Required | Description |
|----------------------|---------|----------|-------------|
| **`type`** | string | ✅ Yes  | Strategy for writing data (`replace`, `append`, `merge`). |
| **`strategy`** | string | ✅ Yes | Merge method (e.g., `delete-insert`,`scd2`,`upsert`). |



##### **Delete-Insert Configuration (`config`)** *(Mandatory when `write_disposition_config.strategy` is `delete-insert`)*
| Field                | Type    | Required | Description |
|----------------------|---------|----------|-------------|
| **`primary_key`**   | string  | ✅ Yes   | Primary key for identifying records. |
| **`merge_key`**     | string  | ✅ Yes   | Key used for merging. |
| **`hard_delete_column`** | string  | ❌ No  | Column used to mark hard deletions. |
| **`dedup_sort_column`**  | object  | ❌ No  | Specifies sorting order for deduplication. |
