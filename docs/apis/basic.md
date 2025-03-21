# /ingest
The ingest api endpoint is the primary endpoint to initiate transfer of data between source and destination.


## Request Payload

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

#### **Resource Fields**
| Field                    | Type    | Required | Description |
|--------------------------|---------|----------|-------------|
| **`source_table_name`**  | string  | ✅ Yes   | Name of the source table. |
| **`destination_table_name`** | string  | ❌ No   | Name of the destination table. Defaults to `source_table_name` |

## Example 
### Move data between Postgres and Clickhouse.
```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:password@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:password@localhost:9000/db_name?http_port=8123&secure=0",
    "resources": [
      {"source_table_name": "users"}
    ]
  }'
```

### **Move data between S3 and Clickhouse.** 
*(Optional `destination_table_name`)*
```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "s3://your-bucket?access_key_id=user_access_key_id&access_key_secret=user_access_key_secret&region=bucket_region",
    "destination_uri": "clickhouse://default:password@localhost:9000/db_name?http_port=8123&secure=0",
    "resources": [
      {
      "source_table_name": "users/data/dump.csv",
      "destination_table_name": "dw_users"
      }
    ]
  }'
```

### Move data between Postgres and Duckdb.
*(Optional `dataset`)*

```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:password@localhost:5432/db_name",
    "destination_uri": "duckdb:///my_database.duckdb",
    "dataset": "main"
    "resources": [
      {
      "source_table_name": "users"      
      }
    ]
  }'
```

### Move multiple tables from Postgres and Clickhouse.

```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:password@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:password@localhost:9000/db_name?http_port=8123&secure=0",
    "dataset": "public"
    "resources": [
      {
      "source_table_name": "users",
      "destination_table_name": "dw_users"      
      },
      {
      "source_table_name": "products",
      "destination_table_name": "dw_products"      
      }
    ]
  }'
```