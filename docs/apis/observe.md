# /observe
Retrieve detailed observability metrics for a given pipeline instance.

## 🧭 Endpoint
GET /ferry/{id}/observe
## 📥 Path Parameters

| Parameter | Type   | Description                      |
|-----------|--------|----------------------------------|
| `id`      | string | A unique identifier for the ingestion job.|


## 📊 Response Fields

### Top-Level

| Field           | Type     | Description                          |
|-----------------|----------|--------------------------------------|
| `pipeline_name` | string   | Name of the pipeline instance        |
| `start_time`    | datetime | Timestamp when the pipeline started  |
| `end_time`      | datetime | Timestamp when the pipeline ended    |
| `status`        | string   | Status of the pipeline run           |
| `error`        | string   | Errors if any in pipeline run           |
| `metrics`       | object   | Detailed observability per phase     |

---

### `metrics.extract`

| Field           | Type         | Description                              |
|-----------------|--------------|------------------------------------------|
| `start_time`    | datetime     | When extract phase started               |
| `end_time`      | datetime     | When extract phase ended                 |
| `status`        | string       | Status of the extract phase              |
| `error`        | object/null  | Errors encountered during extraction     |
| `resource_metrics` | array| List of extracted resource_metrics and metadata    |



### `metrics.normalize`

| Field              | Type         | Description                             |
|--------------------|--------------|-----------------------------------------|
| `start_time`       | datetime     | When normalize phase started            |
| `end_time`         | datetime     | When normalize phase ended              |
| `status`           | string       | Status of the normalize phase           |
| `error`        | object/null  | Errors encountered during extraction     |
| `resource_metrics` | array| Normalized resources and file statistics|

### `metrics.normalize`

| Field              | Type         | Description                             |
|--------------------|--------------|-----------------------------------------|
| `start_time`       | datetime     | When normalize phase started            |
| `end_time`         | datetime     | When normalize phase ended              |
| `status`           | string       | Status of the normalize phase           |
| `error`        | object/null  | Errors encountered during extraction     |

#### `resource_metrics[]` 

| Field        | Type    | Description                |
|--------------|---------|----------------------------|
| `name`       | string  | Resource or table name     |
| `row_count`  | integer | Number of rows output      |
| `file_size`  | integer | File size in bytes         |