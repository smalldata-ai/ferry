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

# Define the test request for Kafka
request = ferry_pb2.IngestRequest(
    identity="test_kafka_268",
    source_uri=(
        "kafka://localhost:9092?"
        "group_id=test_group&"
        "security_protocol=PLAINTEXT&"
        "sasl_mechanisms=PLAIN&"
        "sasl_username=user&"
        "sasl_password=pass&"
        "schema_registry=http://localhost:8081&"
        "use_avro=true"
    ),
    destination_uri="duckdb:////mnt/d/smalldata.ai/Ferry-develop/ferry/Output_db/test_duckdb_kafka268.duckdb",
    resources=[
        ferry_pb2.Resource(
            source_table_name="stroke-avro-topic1",
            destination_table_name="avro_table_stroke",
            column_rules=ferry_pb2.ColumnRules(
                exclude_columns=["id"], pseudonymizing_columns=["stroke"]
            ),
            write_disposition_config=ferry_pb2.WriteDispositionConfig(type="replace"),
            source_options=ferry_pb2.SourceOptions(
                batch_size=100, batch_timeout=5, start_from="earliest"
            ),
        )
    ],
)

# Generate secure metadata
timestamp = str(int(time.time()))
raw_body = request.SerializeToString()
message = f"{timestamp}.{raw_body.hex()}"
signature = hmac.new(client_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
metadata = [("x-client-id", client_id), ("x-timestamp", timestamp), ("x-signature", signature)]

# Connect to gRPC server
channel = grpc.insecure_channel("localhost:50051")
stub = ferry_pb2_grpc.FerryServiceStub(channel)
print(f"Client Signing Message: {timestamp}.{raw_body.hex()}")

# Make gRPC call
try:
    response = stub.IngestData(request, metadata=metadata)
    print(f"Response: {response.status} - {response.message}")
except grpc.RpcError as e:
    print(f"gRPC Error: {e.code()} - {e.details()}")
