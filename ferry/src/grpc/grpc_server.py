import grpc
import logging
from concurrent import futures
from ferry_pb2 import LoadDataResponse
import ferry_pb2_grpc
from ferry.src.tasks import load_data_task
from ferry.src.restapi.config import config  # Import config settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoaderService(ferry_pb2_grpc.DataLoaderServicer):
    def LoadData(self, request, context):
        try:
            logger.info(f"Received gRPC request: {request}")

            request_dict = {
                "source_uri": request.source_uri,
                "source_table_name": request.source_table_name,
                "destination_uri": request.destination_uri,
                "destination_table_name": request.destination_table_name,
                "dataset_name": request.dataset_name  
            }

            logger.info(f"Converted gRPC request to dict: {request_dict}")

            # Send to Celery
            task = load_data_task.delay(request_dict)
            logger.info(f"Task {task.id} started for {request.source_uri} -> {request.destination_uri}")

            return LoadDataResponse(
                status="processing",
                message="Data loading started in the background.",
                task_id=task.id
            )
        except Exception as e:
            logger.exception(f"Error starting Celery task: {e}")
            return LoadDataResponse(
                status="error",
                message=f"Failed to start data loading: {str(e)}",
                task_id=""
            )

def serve():
    """Starts the gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.GRPC_WORKERS))
    ferry_pb2_grpc.add_DataLoaderServicer_to_server(DataLoaderService(), server)
    
    grpc_address = f"[::]:{config.GRPC_PORT}"
    server.add_insecure_port(grpc_address)
    server.start()
    
    logger.info(f" gRPC server started on port {config.GRPC_PORT} with {config.GRPC_WORKERS} workers")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
