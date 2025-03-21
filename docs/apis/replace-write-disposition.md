# /ingest
`write_disposition`=`replace` involves completely refreshing your destination data. 
All existing records are deleted and replaced with the latest data from the source during the current run.
[Refer here](/guides/wd-replace)


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
      "write_disposition": "replace",
      "replace_config": {
        "strategy": "truncate-and-insert"
      }
    }
  ]
}
```

## Parameters Descriptions

#### **Write Disposition (`write_disposition`)** *(Optional)* *(default: replace)*

| Field                | Type    | Required | Description |
|----------------------|---------|----------|-------------|
| **`write_disposition`** | string | ✅ Yes  | Strategy for writing data (`replace`, `append`, `merge`). |

#### **Replace Configuration (`replace_config`)** *(Mandatory when `write_disposition` is `replace`)*
| Field        | Type    | Required | Description |
|-------------|---------|----------|-------------|
| **`strategy`** | string | ✅ Yes | Defines how replacement is handled (e.g., `truncate-and-insert`,`insert-from-staging`,`staging-optimized`). |


### **Move data between S3 and Snowflake.** 
*(strategy `staging-optimized`)*
```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "s3://your-bucket?access_key_id=user_access_key_id&access_key_secret=user_access_key_secret&region=bucket_region",
    "destination_uri": "snowflake://user_name:password@account/dataset",
    "resources": [
      {
      "source_table_name": "/users/data/dump.csv",
      "destination_table_name": "dw_users",
      "write_disposition": "replace",
      "replace_config": {
          "strategy": "staging-optimized"
        }
      }
    ]
  }'
```

### **Move data between Postgres and Motherduck.** 
*(strategy `insert-from-staging`)*
```sh
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "identity": "fgXOw4zY"
    "source_uri": "postgresql://postgres:password@localhost:5432/db_name",
    "destination_uri": "md://db_name?token=your-md-token",
    "resources": [
      {
      "source_table_name": "public.users",
      "destination_table_name": "dw_users",
      "write_disposition": "replace",
      "replace_config": {
          "strategy": "insert-from-staging"
        }
      }
    ]
  }'
```