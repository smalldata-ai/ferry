# /ferry
`write_disposition`=`merge` && `strategy`=`upsert`
<br>
Merges new data into the destination using merge_key and deduplicates new data using primary_key
[Refer here](/guides/merge-upsert)

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
          "strategy": "upsert",
          "config": { 
              "primary_key": "string", 
              "hard_delete_column": "string"
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


##### **Upsert Configuration (`upsert_config`)** *(Mandatory when `merg_config.strategy` is `upsert`)*
| Field                  | Type    | Required | Description |
|------------------------|---------|----------|-------------|
| **`primary_key`**      | string  | ✅ Yes   | Key for identifying records. |
| **`hard_delete_column`** | string  | ❌ No  | Column marking records for deletion. |