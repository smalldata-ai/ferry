Got it! Let’s walk through how to **properly structure and write documentation inside `docs/gRPC/`** for your gRPC feature using **VitePress**.

Since this is your first time, I’ll cover:

---

## ✅ 1. Directory Structure

Here's how your `docs/` folder (especially for `gRPC`) should look:

```
.vitepress/
docs/
├── gRPC/
│   ├── index.md               ← Main gRPC documentation page
│   └── client-example.md      ← Example of using the gRPC client
```

---

## ✅ 2. Sidebar & Navigation

You've already added this to `.vitepress/config.mts`:

```ts
{ text: 'gRPC', link: '/gRPC/coming-soon' },
```

But since we’re replacing "coming-soon" with real content, update that to:

```ts
{ text: 'gRPC', link: '/gRPC/' }
```

---

## ✅ 3. `docs/gRPC/index.md` – gRPC Overview Page

Create this file with the following content:

```md
# 🚀 gRPC Integration

Ferry supports high-performance data ingestion using **gRPC**. This enables secure, efficient, and scalable communication between clients and the Ferry ingestion engine.

## 🔧 How to Start gRPC Server

You can run the gRPC server using the following commands:

### Basic (default port `50051`)
```bash
python ferry/main.py serve-grpc
```

### Custom Port
```bash
python ferry/main.py serve-grpc --port 60000
```

### Secure Connection
```bash
python ferry/main.py serve-grpc --port 60000 --secure
```

> 💡 You can combine flags as needed for your deployment setup.

---

## 📦 Protobuf Definitions

All gRPC services and messages are defined in:
```
ferry/protos/ferry.proto
```

Make sure the generated code exists in:
- `ferry/protos/ferry_pb2.py`
- `ferry/protos/ferry_pb2_grpc.py`

Use `grpcio-tools` to regenerate them if needed:
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ferry.proto
```

---

## 📞 Available RPCs

### `IngestData`

- **Request Type**: `IngestRequest`
- **Response Type**: `IngestResponse`

This endpoint initiates a data ingestion task.

See [Client Example »](./client-example.md) for how to use it.
```

---

## ✅ 4. `docs/gRPC/client-example.md` – gRPC Client Example

```md
# 🧪 gRPC Client Example

Here’s a working Python client example for calling the gRPC `IngestData` method in Ferry.

```python
import time
import hmac
import hashlib
import grpc
from protos import ferry_pb2
from protos import ferry_pb2_grpc
from ferry.src.security import SecretsManager

# Get credentials
creds = SecretsManager.get_credentials()
client_id = creds.client_id
client_secret = creds.client_secret

# Define the request
request = ferry_pb2.IngestRequest(
    identity="test_s3_249",
    source_uri="s3://fynd-development?...",
    destination_uri="duckdb:///.../s3_test247.duckdb",
    resources=[
        ferry_pb2.Resource(
            source_table_name="data.csv",
            destination_table_name="data_csv_table",
            column_rules=ferry_pb2.ColumnRules(
                exclude_columns=["id", "stroke"],
                pseudonymizing_columns=["hypertension", "bmi"]
            ),
            write_disposition_config=ferry_pb2.WriteDispositionConfig(type="replace"),
        )
        # Add more Resource blocks as needed
    ],
)

# Sign the request
timestamp = str(int(time.time()))
raw_body = request.SerializeToString()
message = f"{timestamp}.{raw_body.hex()}"
signature = hmac.new(client_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
metadata = [
    ("x-client-id", client_id),
    ("x-timestamp", timestamp),
    ("x-signature", signature)
]

# Call the server
channel = grpc.insecure_channel("localhost:50051")
stub = ferry_pb2_grpc.FerryServiceStub(channel)
print(f"Signing message: {timestamp}.{raw_body.hex()}")

try:
    response = stub.IngestData(request, metadata=metadata)
    print(f"Response: {response.status} - {response.message}")
except grpc.RpcError as e:
    print(f"gRPC Error: {e.code()} - {e.details()}")
```

---

## 🛡️ Authentication

Each gRPC request must include:
- `x-client-id`
- `x-timestamp`
- `x-signature` (HMAC SHA256 of timestamp + serialized body)

These headers are used by the server to validate and authorize the request.

---

## 📌 Notes

- Use `grpcio` and `grpcio-tools` for Python development.
- Make sure your `ferry.proto` matches the client's generated classes.
- The above example uses insecure mode for local testing.
```

---

