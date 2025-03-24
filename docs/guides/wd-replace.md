# Replace
Replace Write Disposition is the default option, and supports three strategies for replacing data in destination tables:

- `truncate-and-insert` (default)
- `insert-from-staging`
- `staging-optimized`

## Strategies
### truncate-and-insert (default)
The truncate-and-insert strategy is the default and the fastest of the three strategies.

- Before loading new data, the destination tables are truncated.
- New data is inserted consecutively but not within the same transaction.
- If the load fails midway, some tables may be updated while others remain empty.
- If avoiding data downtime is a priority, consider using `insert-from-staging` or `staging-optimized`.

### insert-from-staging
The insert-from-staging strategy ensures zero downtime and consistent state for nested and root tables.
- New data is first loaded into staging tables.
- The final destination tables are only updated in a single transaction.
- Works the same way across all destinations.
- This strategy is the slowest but ensures data consistency at all times.

### staging-optimized
The staging-optimized strategy builds upon `insert-from-staging` but includes optimizations for faster loading.

- The final destination tables may be dropped and recreated instead of being truncated.
- It provides a performance boost but may remove existing views, constraints, or indexes.
- Use this strategy only if you do not need to retain table structures.

## Request Payload
```js
{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:@localhost:5432/db_name",
    "destination_uri": "clickhouse://default:@localhost:9000/dlt?http_port=8123&secure=0",
    "resources": [
      {
        "source_table_name": "users",
        "write_disposition": "replace", // [!code focus]
        "replace_config": { // [!code focus]
          "strategy": "insert-from-staging" // [!code focus]
        } // [!code focus]
      }
    ]
  }
```