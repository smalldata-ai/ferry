# Getting Started with gRPC

Ferry exposes a gRPC interface for secure, high-performance communication. It enables remote triggering of ingestion workflows and retrieval of pipeline observability metrics.


---

## 🚀 Starting the gRPC Server

Use the CLI to start the gRPC server:

```bash
python ferry/main.py serve-grpc
```

By default, this starts the server on port `50051`.

### Change the port to anuthing you want
python ferry/main.py serve-grpc --port 60000


### 🔐 Secure Mode (HMAC Authentication)

To enable secure mode (recommended in production):

```bash
python ferry/main.py serve-grpc --secure
```

> In secure mode, the server uses HMAC-based request validation. You'll need a valid `client_id` and `client_secret` in `.ferry/server_secrets.json`.

Example `server_secrets.json`:

```json
{
  "client_id": "my-client",
  "client_secret": "super-secret-key"
}
```

---

## 🛠️ gRPC Services Overview

Ferry exposes the following gRPC services:

### `IngestData`

Triggers ingestion from a source to a destination, using the configuration provided.

#### Request Fields:
- `identity`: Unique pipeline name
- `source_uri`: Source connector string
- `destination_uri`: Destination connector string
- `resources`: One or more tables with:
  - `source_table_name`
  - `destination_table_name` *(optional)*
  - `column_rules` *(exclude, pseudonymize)*
  - `write_disposition_config` *(append, replace, merge)*

#### Example Payload (Python):

```python
from ferry_pb2 import IngestRequest, Resource, ColumnRules, WriteDispositionConfig

req = IngestRequest(
    identity="my_pipeline",
    source_uri="postgresql://...",
    destination_uri="s3://...",
    resources=[
        ferry_pb2.Resource(
            source_table_name="users",
            destination_table_name="clean_users",
            column_rules=ColumnRules(                               #optional declaration

                exclude_columns=["email", "phone"],
                pseudonymizing_columns=["name"]
            ),
            write_disposition_config=WriteDispositionConfig(type="merge")
        )
    ]
)
```

---

### `GetObservability`

Returns detailed live metrics and status of a given pipeline execution.

```python

import grpc
from ferry.src.grpc.protos import ferry_pb2, ferry_pb2_grpc

channel = grpc.insecure_channel("localhost:50051")
stub = ferry_pb2_grpc.FerryServiceStub(channel)

request = ferry_pb2.ObservabilityRequest(identity="PIPELINE_NAME")
response = stub.GetObservability(request)

# print("Status:", response.status)
print("Metrics:", response.metrics)
```

#### Request Field:
- `identity`: The pipeline name to fetch metrics for.

#### Response:
Returns:
- start and end times
- extract/normalize/load stats
- pipeline status
- source/destination types
- any error message

---

## 🧪 Authentication (When Secure Mode is On)

Your client must attach the following headers:

- `x-client-id`
- `x-timestamp` (UNIX epoch)
- `x-signature` (HMAC-SHA256 of `timestamp.payload` using `client_secret`)

#### Python Example:

```python
import hmac
import hashlib
import time
import grpc
from ferry_pb2 import IngestRequest
from ferry_pb2_grpc import FerryServiceStub

payload = IngestRequest(...).SerializeToString()
timestamp = str(int(time.time()))
message = f"{timestamp}.{payload.hex()}"

signature = hmac.new(
    b"super-secret-key",
    message.encode(),
    hashlib.sha256
).hexdigest()

metadata = [
    ("x-client-id", "my-client"),
    ("x-timestamp", timestamp),
    ("x-signature", signature)
]

channel = grpc.insecure_channel("localhost:50051")
stub = FerryServiceStub(channel)
response = stub.IngestData(payload, metadata=metadata)
```

---

## 📂 Folder Structure

- `grpc_server.py`: Main server logic
- `protos/ferry.proto`: Service and message definitions
- `serve_grpc` command: Starts the server via CLI
- `.ferry/server_secrets.json`: Secrets for HMAC validation (optional, only in secure mode)

---

## 📊 Observability Output Example

Example JSON response from `GetObservability`:

```json
{
  "pipeline_name": "my_pipeline",
  "start_time": "2025-04-14T10:00:00",
  "end_time": "2025-04-14T10:00:05",
  "status": "SUCCESS",
  "destination_type": "s3",
  "source_type": "postgres",
  "metrics": {
    "extract": {"records": 1200, "duration_s": 1.2},
    "normalize": {"records": 1200, "duration_s": 0.5},
    "load": {"records": 1200, "duration_s": 2.0}
  }
}
```