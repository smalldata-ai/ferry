# Append
Append Write Disposition allows new data to be added to an existing table without modifying or deleting previous records. 
It is useful for logging, time-series data, and [incremental data](/guides/incremental-loading) ingestion scenarios. 
This mode ensures historical data is preserved while continuously adding new entries.

## Request Payload
```js
{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:@localhost:9000/dlt?http_port=8123&secure=0",
    "resources": [
      {
        "source_table_name": "logs",
        "write_disposition_config": { // [!code focus]
          "type": "append" // [!code focus]
        } // [!code focus]
      }
    ]
  }
```