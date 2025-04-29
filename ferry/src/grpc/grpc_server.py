import grpc
import argparse
import logging
import time
import hmac
import hashlib
import json
from concurrent import futures
from ferry.src.grpc.protos import ferry_pb2, ferry_pb2_grpc
from ferry.src.pipeline_builder import PipelineBuilder
from ferry.src.pipeline_metrics import PipelineMetrics
from ferry.src.data_models.ingest_model import IngestModel, ResourceConfig, WriteDispositionConfig
import os

# Global authentication state
CLIENT_ID = None
CLIENT_SECRET = None
SECURE_MODE = False


# Load stored client secrets
def load_client_secrets():
    global CLIENT_ID, CLIENT_SECRET
    secret_path = os.path.join("ferry", ".ferry", "server_secrets.json")  # Corrected path
    try:
        with open(secret_path, "r") as f:
            secrets = json.load(f)
            CLIENT_ID = secrets.get("client_id")
            CLIENT_SECRET = secrets.get("client_secret")
    except FileNotFoundError:
        logging.warning("No client secrets found. Authentication disabled.")


def validate_request_metadata(metadata, request_body):
    headers = {key: value for key, value in metadata}
    client_id = headers.get("x-client-id")
    timestamp = headers.get("x-timestamp")
    signature = headers.get("x-signature")

    logging.info(f"Received client_id: {client_id}, timestamp: {timestamp}, signature: {signature}")
    logging.info(f"Server Signing Message: {timestamp}.{request_body.hex()}")

    if not client_id or not timestamp or not signature:
        logging.warning("Missing authentication headers")
        return grpc.StatusCode.INVALID_ARGUMENT, "Missing authentication headers"

    if client_id != CLIENT_ID:
        logging.warning(f"Unknown client ID: {client_id}, expected: {CLIENT_ID}")
        return grpc.StatusCode.UNAUTHENTICATED, "Unknown client ID"

    try:
        timestamp = int(timestamp)
        if abs(time.time() - timestamp) > 300:  # 5-minute expiry
            logging.warning("Expired timestamp")
            return grpc.StatusCode.UNAUTHENTICATED, "Expired timestamp"
    except ValueError:
        logging.warning("Invalid timestamp format")
        return grpc.StatusCode.UNAUTHENTICATED, "Invalid timestamp format"

    expected_signature = hmac.new(
        CLIENT_SECRET.encode(), f"{timestamp}.{request_body.hex()}".encode(), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(expected_signature, signature):
        logging.warning(f"Signature mismatch: {expected_signature} != {signature}")
        return grpc.StatusCode.UNAUTHENTICATED, "Signature mismatch"

    return None, None


class FerryServiceServicer(ferry_pb2_grpc.FerryServiceServicer):
    def IngestData(self, request, context):
        if SECURE_MODE:
            status, error_msg = validate_request_metadata(
                context.invocation_metadata(), request.SerializeToString()
            )
            if status:
                context.set_code(status)
                context.set_details(error_msg)
                return ferry_pb2.IngestResponse(status="ERROR", message=error_msg)

        try:
            ingest_model = IngestModel(
                identity=request.identity,
                source_uri=request.source_uri,
                destination_uri=request.destination_uri,
                resources=[
                    ResourceConfig(
                        source_table_name=res.source_table_name,
                        destination_table_name=res.destination_table_name or None,
                        column_rules={
                            "exclude_columns": list(res.column_rules.exclude_columns),
                            "pseudonymizing_columns": list(res.column_rules.pseudonymizing_columns),
                        }
                        if res.HasField("column_rules")
                        else None,
                        write_disposition_config=WriteDispositionConfig(
                            type=res.write_disposition_config.type
                        )
                        if res.HasField("write_disposition_config")
                        else None,
                    )
                    for res in request.resources
                ],
            )
            pipeline = PipelineBuilder(model=ingest_model).build()
            pipeline.run()
            return ferry_pb2.IngestResponse(
                status="SUCCESS",
                message="Data ingestion completed successfully",
                pipeline_name=pipeline.get_name(),
            )
        except Exception as e:
            logging.exception("Error in IngestData")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return ferry_pb2.IngestResponse(status="ERROR", message=str(e))

    def GetObservability(self, request, context):
        if SECURE_MODE:
            status, error_msg = validate_request_metadata(
                context.invocation_metadata(), request.SerializeToString()
            )
            if status:
                context.set_code(status)
                context.set_details(error_msg)
                return ferry_pb2.ObservabilityResponse(status="ERROR", metrics="")

        retries = 3
        delay = 1.5  # seconds

        for attempt in range(retries):
            try:
                raw_metrics = PipelineMetrics(name=request.identity).generate_metrics()

                if (
                    raw_metrics["status"] == "error"
                    or "not found" in (raw_metrics.get("error") or "").lower()
                ):
                    if attempt < retries - 1:
                        time.sleep(delay)
                        continue
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(f"Pipeline '{request.identity}' not found")
                    return ferry_pb2.ObservabilityResponse(status="ERROR", metrics="")

                # Reorder the metrics section
                ordered_metrics = {
                    "extract": raw_metrics["metrics"].get("extract"),
                    "normalize": raw_metrics["metrics"].get("normalize"),
                    "load": raw_metrics["metrics"].get("load"),
                }

                # Rebuild the full metrics dict with ordered metrics
                ordered_full = {
                    "pipeline_name": raw_metrics.get("pipeline_name"),
                    "start_time": raw_metrics.get("start_time"),
                    "end_time": raw_metrics.get("end_time"),
                    "status": raw_metrics.get("status"),
                    "destination_type": raw_metrics.get("destination_type"),
                    "source_type": raw_metrics.get("source_type"),
                    "error": raw_metrics.get("error"),
                    "metrics": ordered_metrics,
                }

                return ferry_pb2.ObservabilityResponse(
                    status="SUCCESS",
                    metrics=json.dumps(ordered_full, indent=2, default=str),
                )

            except Exception as e:
                logging.warning(f"Observability fetch failed on attempt {attempt + 1}: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                    continue
                logging.exception("Error in GetObservability")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return ferry_pb2.ObservabilityResponse(status="ERROR", metrics="")


def serve(port, secure_mode):
    global SECURE_MODE
    SECURE_MODE = secure_mode
    load_client_secrets()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ferry_pb2_grpc.add_FerryServiceServicer_to_server(FerryServiceServicer(), server)
    server.add_insecure_port(f"[::]:{port}")

    logging.info(f"Starting gRPC server on port {port} with secure mode: {secure_mode}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the Ferry gRPC server.")
    parser.add_argument("--port", type=int, default=50051, help="Port to run the gRPC server on")
    parser.add_argument("--secure", action="store_true", help="Enable secure authentication mode")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    serve(args.port, args.secure)
