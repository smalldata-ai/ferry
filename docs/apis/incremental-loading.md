# /ferry
Including `incremental_config` enables the ferry endpoint to fetch data incrementally from the source.
[Refer](/guides/incremental-loading)

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
      "incremental_config": { // [!code focus]
        "incremental_key": "string", // [!code focus]
        "start_position": 0, // [!code focus]
        "end_position": 0, // [!code focus]
        "lag_window": 0, // [!code focus]
        "boundary_mode": "start-end" // [!code focus]
      },
    }
  ]
}
```

## Parameters Descriptions

#### **Incremental Configuration (`incremental_config`)** *(Optional)*
| Field                  | Type    | Required | Description |
|------------------------|---------|----------|-------------|
| **`incremental_key`**  | string  | ✅ Yes   | The column used for incremental loading. |
| **`start_position`**   | integer | ❌ No   | The starting point for incremental extraction. |
| **`end_position`**     | integer | ❌ No   | The endpoint for incremental extraction. *If end_position is provided, it will assume that the data needs to be backfilled from start_position to end_position and will maintain no incremental state.*|
| **`lag_window`**       | integer | ❌ No   | A buffer to ensure data consistency. |
| **`boundary_mode`**    | string  | ❌ No   | Defines the mode (`start-end`, etc.). |


## Example 
### Move data incrementally between Postgres and Clickhouse.
```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:password@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:password@localhost:9000/db_name?http_port=8123&secure=0",
    "resources": [
      {
        "source_table_name": "users",
        "write_disposition": "append",
        "incremental_config": { 
          "incremental_key": "id"
        }
      }
    ]
  }'
```

### Backfill data between Postgres and Clickhouse.
```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:password@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:password@localhost:9000/db_name?http_port=8123&secure=0",
    "resources": [
      {
        "source_table_name": "users",
        "write_disposition": "append",
        "incremental_config": { 
          "incremental_key": "registered_at",
          "start_position": "2020-01-01",
          "end_position": "2021-01-01"
        }
      }
    ]
  }'
```