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
| **`end_position`**     | integer | ❌ No   | The endpoint for incremental extraction. |
| **`lag_window`**       | integer | ❌ No   | A buffer to ensure data consistency. |
| **`boundary_mode`**    | string  | ❌ No   | Defines the mode (`start-end`, etc.). |
