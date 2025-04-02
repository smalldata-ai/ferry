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
    identity="output_033",
    source_uri="s3://<bucketname>?access_key_id=<>&access_key_secret=<>&region=<>",
    destination_uri="duckdb:////mnt/d/smalldata.ai/ferry-develop/ferry/output_033.duckdb",
    resources=[
        ferry_pb2.Resource(
            source_table_name="data.csv",
            destination_table_name="data_table",
            column_rules=ferry_pb2.ColumnRules(
                exclude_columns=["hypertension", "stroke"], pseudonymizing_columns=["id", "bmi"]
            ),
            write_disposition_config=ferry_pb2.WriteDispositionConfig(type="replace"),
        )
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
channel = grpc.insecure_channel("localhost:60000")
stub = ferry_pb2_grpc.FerryServiceStub(channel)
print(f"Client Signing Message: {timestamp}.{raw_body.hex()}")

# Make the gRPC call with metadata
try:
    response = stub.IngestData(request, metadata=metadata)
    print(f"Response: {response.status} - {response.message}")
except grpc.RpcError as e:
    print(f"gRPC Error: {e.code()} - {e.details()}")
