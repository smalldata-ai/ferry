# /ingest



## Example
This api request body only moves incremental data between source and destination.
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
      "write_disposition": "replace",
      "replace_config": {
        "strategy": "truncate-and-insert"
      }
    }
  ]
}
```

## Parameters Descriptions


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