import logging
from fastapi import HTTPException
import dlt
# from ferry.src.restapi.collector import LogCollector

from ferry.src.destination_factory import DestinationFactory
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.source_factory import SourceFactory
from ferry.src.logging.ferry_log_collector import FerryLogCollector  # Fixed import

logger = logging.getLogger(__name__)

def debug_uris(request):
    """Logs source and destination URIs for debugging."""
    logger.info(f"Received source_uri: {request.source_uri}")
    logger.info(f"Received destination_uri: {request.destination_uri}")

def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str, task_id: str) -> dlt.Pipeline:
    """Initializes the DLT pipeline dynamically based on destination"""
    try:
        # Retrieve the destination dynamically using DestinationFactory
        destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)

        # Pass task_id to FerryLogCollector
        return dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset_name,
            dev_mode=True, 
            progress=FerryLogCollector(task_id=task_id)  # ✅ Fix applied here
        )

    except Exception as e:
        logger.exception(f"Failed to create pipeline: {e}")
        raise RuntimeError(f"Pipeline creation failed: {str(e)}")


async def load_data_endpoint(request: LoadDataRequest, task_id: str) -> LoadDataResponse:
    """Triggers the Extraction, Normalization, and Loading of data from source to destination"""
    try:
        # Log the parsed request
        logger.info(f" Parsed request: {request.model_dump_json()}")  

        # Determine pipeline name dynamically based on the source and destination URIs
        source_scheme = request.source_uri.split(":")[0]
        destination_scheme = request.destination_uri.split(":")[0]
        pipeline_name = f"{source_scheme}_to_{destination_scheme}"

        logger.info(f"Pipeline name resolved as: {pipeline_name}")

        # Create the pipeline dynamically and pass the task_id
        pipeline = create_pipeline(pipeline_name, request.destination_uri, request.dataset_name, task_id)

        # Get the source system based on source URI
        source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)

        # Run the pipeline
        pipeline.run(source, table_name=request.destination_table_name, write_disposition="replace")

        return LoadDataResponse(
            status="success",
            message="Data loaded successfully",
            pipeline_name=pipeline.pipeline_name,
            table_processed=request.destination_table_name,
        ).model_dump()

    except Exception as e:
        logger.exception(f" Unexpected error in load_data_endpoint: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred")
