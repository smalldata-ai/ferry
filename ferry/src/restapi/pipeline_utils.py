import logging
from fastapi import HTTPException
import dlt
# from ferry.src.restapi.collector import LogCollector
from dlt.common.runtime.collector import Collector
  
# import time
# from dlt.common import logger as dlt_logger
# from collections import defaultdict
# import sys
from ferry.src.destination_factory import DestinationFactory
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.source_factory import SourceFactory
# import os



logger = logging.getLogger(__name__)

import logging

class CustomLogCollector(Collector):
    """Custom Log Collector to track pipeline execution progress."""

    def __init__(self):
        self.logs = []  # Store logs in memory

    def __call__(self, log_entry):
        """This method gets called with each log update from DLT."""
        self.logs.append(log_entry)
        logging.info(f"Pipeline Log: {log_entry}")  # Log to standard logging

    def get_logs(self):
        """Returns collected logs for further processing."""
        return self.logs

    def __enter__(self):
        """Called when entering the context (with statement)."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Called when exiting the context (with statement)."""
        if exc_type:
            logging.error(f"Exception in CustomLogCollector: {exc_value}")


def debug_uris(request):
    """Logs source and destination URIs for debugging."""
    logger.info(f"Received source_uri: {request.source_uri}")
    logger.info(f"Received destination_uri: {request.destination_uri}")

def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
    """Initializes the DLT pipeline dynamically based on destination"""
    try:
        # Retrieve the destination dynamically using DestinationFactory
        destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)
        
        # ✅ Use LogCollector() for progress tracking
        return dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset_name,
            progress=CustomLogCollector()
        )
        
    except Exception as e:
        logger.exception(f"Failed to create pipeline: {e}")
        raise RuntimeError(f"Pipeline creation failed: {str(e)}")

async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
    """Triggers the Extraction, Normalization, and Loading of data from source to destination"""
    try:
        # Log the parsed request
        logger.info(f"✅ Parsed request: {request.model_dump_json()}")  

        # Determine pipeline name dynamically based on the source and destination URIs
        source_scheme = request.source_uri.split(":")[0]
        destination_scheme = request.destination_uri.split(":")[0]
        
        # If no specific formatting needed, fallback to the generic scheme naming
        pipeline_name = f"{source_scheme}_to_{destination_scheme}"

        logger.info(f"Pipeline name resolved as: {pipeline_name}")

        # Create the pipeline dynamically based on the destination URI
        pipeline = create_pipeline(pipeline_name, request.destination_uri, request.dataset_name)

        # Get the source system based on source URI
        source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)

        # Run the pipeline
        pipeline.run(source, table_name=request.destination_table_name, write_disposition="replace")

        return LoadDataResponse(
            status="success",
            message="Data loaded successfully",
            pipeline_name=pipeline.pipeline_name,
            table_processed=request.destination_table_name,
        ).model_dump()  # Convert Pydantic model to a dictionary

    except Exception as e:
        logger.exception(f"❌ Unexpected error in load_data_endpoint: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred")
