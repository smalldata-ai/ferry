# /ferry
`write_disposition`=`append` 
<br>
Appends new data to the destination.This is suitable for stateless data, which doesn’t require modification.
[Refer here](/guides/wd-append)

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
      "write_disposition_config": { 
          "type": "append"
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
