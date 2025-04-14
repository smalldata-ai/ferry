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

# Define the test request
request = ferry_pb2.IngestRequest(
    identity="test_s3_249",
    source_uri="",
    destination_uri="duckdb:///D:/smalldata.ai/Ferry-develop/ferry/Output_db/s3_test247.duckdb",
    resources=[
        ferry_pb2.Resource(
            source_table_name="data.csv",
            destination_table_name="data_csv_table",
            column_rules=ferry_pb2.ColumnRules(
                exclude_columns=["id", "stroke"], pseudonymizing_columns=["hypertension", "bmi"]
            ),
            write_disposition_config=ferry_pb2.WriteDispositionConfig(type="replace"),
        ),
        ferry_pb2.Resource(
            source_table_name="jsonl_data.jsonl",
            destination_table_name="jsonl_data_table",
            column_rules=ferry_pb2.ColumnRules(
                exclude_columns=["Amount"], pseudonymizing_columns=["Country"]
            ),
            write_disposition_config=ferry_pb2.WriteDispositionConfig(type="replace"),
        ),
        ferry_pb2.Resource(
            source_table_name="chocolate_data.parquet",
            destination_table_name="chocolate_data_table",
            column_rules=ferry_pb2.ColumnRules(
                exclude_columns=["Country", "Date"], pseudonymizing_columns=["Product", "Amount"]
            ),
            write_disposition_config=ferry_pb2.WriteDispositionConfig(type="replace"),
        ),
    ],
)

# Get current timestamp
timestamp = str(int(time.time()))

# Serialize the request body
raw_body = request.SerializeToString()

# Prepare the message for HMAC signing
message = f"{timestamp}.{raw_body.hex()}"
signature = hmac.new(client_secret.encode(), message.encode(), hashlib.sha256).hexdigest()

# Add headers for the request
metadata = [("x-client-id", client_id), ("x-timestamp", timestamp), ("x-signature", signature)]

# Connect to the gRPC server
channel = grpc.insecure_channel("localhost:50051")
stub = ferry_pb2_grpc.FerryServiceStub(channel)
print(f"Client Signing Message: {timestamp}.{raw_body.hex()}")

# Make the gRPC call with metadata
try:
    response = stub.IngestData(request, metadata=metadata)
    print(f"Response: {response.status} - {response.message}")
except grpc.RpcError as e:
    print(f"gRPC Error: {e.code()} - {e.details()}")
