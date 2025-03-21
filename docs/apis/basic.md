# /ingest



## Example
This is a simple api request to move data between source database to a destination data warehouse.
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
| **`dataset_name`**   | string  | ❌ No   | The dataset where the data will be stored. If this is not provided it will fallback to the destination db default schema name.|
| **`resources`**      | array   | ✅ Yes   | A list of resources & the ingestion configuration. |

---

### **Resources Array**
Each item in the `resources` array defines how a specific resources should be ingested.

#### **Table Mapping**
| Field                    | Type    | Required | Description |
|--------------------------|---------|----------|-------------|
| **`source_table_name`**  | string  | ✅ Yes   | Name of the source table. |
| **`destination_table_name`** | string  | ❌ No   | Name of the destination table. Defaults to `source_table_name` |

