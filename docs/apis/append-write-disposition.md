# /ferry
`write_disposition`=`append` appends new data to the destination. 
This is suitable for stateless data, which doesn’t require modification.
[Refer here](/guides/wd-append)

## Example
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
      "write_disposition": "append"
      
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
