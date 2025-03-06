import logging
from fastapi import HTTPException
import dlt
# from ferry.src.restapi.collector import LogCollector
from dlt.common.runtime.collector import Collector
import time
from dlt.common import logger as dlt_logger
from collections import defaultdict
import sys
from ferry.src.destination_factory import DestinationFactory
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.source_factory import SourceFactory

from typing import (
    
    Dict,
    DefaultDict,
    NamedTuple,
    Optional,
    Union,
    TextIO,
    
)
logger = logging.getLogger(__name__)


import logging
import time
from abc import ABC
from collections import defaultdict
from typing import Dict, Optional, Union, Type, Any, NamedTuple


class FerryLogCollector(Collector):
    """A Collector that tracks extract/normalize/load progress and logs status."""

    class StageInfo(NamedTuple):
        description: str
        start_time: float
        total_records: Optional[int] = None
        processed_records: int = 0
        status: str = "In Progress"

    def __init__(self, logger: Union[logging.Logger, None] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        self.stages: Dict[str, FerryLogCollector.StageInfo] = {}
        self.last_log_time: float = time.time()

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = None,
    ) -> None:
        """Overrides the Collector's update function to track pipeline stages"""

        stage_key = f"{name}_{label}" if label else name

        if stage_key not in self.stages:
            self.stages[stage_key] = FerryLogCollector.StageInfo(
                description=stage_key,
                start_time=time.time(),
                total_records=total,
                processed_records=0,
                status="In Progress"
            )

        # Update records processed
        stage_info = self.stages[stage_key]
        processed_records = stage_info.processed_records + inc

        # Update total records if needed
        total_records = stage_info.total_records
        if inc_total and total_records is not None:
            total_records += inc_total

        # Determine stage completion status
        status = "In Progress"
        if total_records is not None and processed_records >= total_records:
            status = "Completed"

        # Store updated stage information
        self.stages[stage_key] = FerryLogCollector.StageInfo(
            description=stage_info.description,
            start_time=stage_info.start_time,
            total_records=total_records,
            processed_records=processed_records,
            status=status
        )

        # Log progress
        self.maybe_log(stage_key)

    def maybe_log(self, stage_key: str) -> None:
        """Logs stage progress periodically based on log intervals."""
        current_time = time.time()
        if current_time - self.last_log_time >= 1.0:  # Log every 1 second
            self.log_stage_progress(stage_key)
            self.last_log_time = current_time

    def log_stage_progress(self, stage_key: str) -> None:
        """Logs the progress of a given pipeline stage."""
        current_time = time.time()
        stage_info = self.stages.get(stage_key)

        if not stage_info:
            return

        elapsed_time = current_time - stage_info.start_time
        records_processed = stage_info.processed_records
        total_records = stage_info.total_records
        progress = f"{records_processed}/{total_records}" if total_records else str(records_processed)
        percentage = f"({(records_processed / total_records * 100):.1f}%)" if total_records else ""
        rate = (records_processed / elapsed_time) if elapsed_time > 0 else 0

        log_message = (
            f"[{stage_info.description}] Records Processed: {progress} {percentage} | "
            f"Status: {stage_info.status} | Time: {elapsed_time:.2f}s | Rate: {rate:.2f}/s"
        )

        self._log(logging.INFO, log_message)

    def _log(self, log_level: int, log_message: str) -> None:
        """Handles logging output."""
        if isinstance(self.logger, logging.Logger):
            self.logger.log(log_level, log_message)
        else:
            print(log_message)

    def _start(self, step: str) -> None:
        """Starts tracking a stage."""
        self.stages[step] = FerryLogCollector.StageInfo(
            description=step,
            start_time=time.time(),
            total_records=None,
            processed_records=0,
            status="In Progress"
        )

    def _stop(self) -> None:
        """Stops tracking and logs final progress."""
        for stage_key in self.stages:
            self.log_stage_progress(stage_key)


def debug_uris(request):
    """Logs source and destination URIs for debugging."""
    logger.info(f"Received source_uri: {request.source_uri}")
    logger.info(f"Received destination_uri: {request.destination_uri}")

def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
    """Initializes the DLT pipeline dynamically based on destination"""
    try:
        # Retrieve the destination dynamically using DestinationFactory
        destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)
        
        #  Use LogCollector() for progress tracking
        return dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset_name,
            progress=FerryLogCollector()
        )
        
    except Exception as e:
        logger.exception(f"Failed to create pipeline: {e}")
        raise RuntimeError(f"Pipeline creation failed: {str(e)}")

async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
    """Triggers the Extraction, Normalization, and Loading of data from source to destination"""
    try:
        # Log the parsed request
        logger.info(f" Parsed request: {request.model_dump_json()}")  

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
        logger.exception(f" Unexpected error in load_data_endpoint: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred")
