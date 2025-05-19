# /ferry
`write_disposition`=`merge` && `strategy`=`scd2`
<br>
Tracks historical changes by inserting new records with versioning or timestamps.
A merge operation compares source and target data, inserting new rows for changes while preserving existing records.
[Refer here](/guides/merge-scd2)

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
          "type": "merge" 
          "strategy": "scd2"
          "config": { 
            "natural_merge_key": "string",
            "partition_merge_key": "string",
            "validity_column_names": [
              "valid_from",
              "valid_to"
            ],
            "active_record_timestamp": "9999-12-31",
            "use_boundary_timestamp": false
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



##### **Slowly Changing Dimension Type 2 (`config`)** *(Mandatory when `merg_config.strategy` is `scd2`)*
| Field                     | Type    | Required | Description |
|---------------------------|---------|----------|-------------|
| **`natural_merge_key`**   | string  | ✅ Yes   | Key used for merging historical data. |
| **`partition_merge_key`** | string  | ✅ Yes   | Partition key for merges. |
| **`validity_column_names`** | array | ✅ Yes   | Column names for validity range. |
| **`active_record_timestamp`** | string | ❌ No | Default timestamp for active records. |
| **`use_boundary_timestamp`** | boolean | ❌ No | Whether to use boundary timestamps. |
