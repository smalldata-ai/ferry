import logging
from fastapi import HTTPException
import dlt
# from ferry.src.restapi.collector import LogCollector
from dlt.common.runtime.collector import Collector
import time
import re

from ferry.src.destination_factory import DestinationFactory
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.source_factory import SourceFactory

from typing import (
    
    Dict,
    NamedTuple,
    Optional,
    Union,
)

logger = logging.getLogger(__name__)

class FerryLogCollector(Collector):
    class StageInfo(NamedTuple):
        description: str
        start_time: float
        total_records: Optional[int] = None
        processed_records: int = 0
        status: str = "In Progress"
        stage_type: str = "Unknown"

    def __init__(self, logger: Union[logging.Logger, None] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.stages: Dict[str, FerryLogCollector.StageInfo] = {}
        self.last_log_time: float = time.time()

    def update(self, name: str, inc: int = 1, total: int = None, inc_total: int = None, message: str = None, label: str = None) -> None:
        stage_type = self._categorize_stage(name)
        stage_key = f"{stage_type}: {name}" if stage_type.lower() not in name.lower() else f"{name}"

        if stage_key not in self.stages:
            self.stages[stage_key] = FerryLogCollector.StageInfo(
                description=stage_key,
                start_time=time.time(),
                total_records=total,
                processed_records=0,
                status="In Progress",
                stage_type=stage_type
            )

        stage_info = self.stages[stage_key]
        processed_records = stage_info.processed_records + inc
        total_records = stage_info.total_records
        
        if inc_total and total_records is not None:
            total_records += inc_total
        
        status = "Completed" if total_records and processed_records >= total_records else "In Progress"
        
        self.stages[stage_key] = FerryLogCollector.StageInfo(
            description=stage_info.description,
            start_time=stage_info.start_time,
            total_records=total_records,
            processed_records=processed_records,
            status=status,
            stage_type=stage_type
        )

        self.maybe_log(update=True)

    def maybe_log(self, update=False) -> None:
        current_time = time.time()
        if update or (current_time - self.last_log_time >= 0.10):
            self.log_stage_progress(update)
            self.last_log_time = current_time

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


    def log_stage_progress(self, update=False) -> None:
        current_time = time.time()
        log_messages = []
        
        for stage_key, stage_info in self.stages.items():
            elapsed_time = current_time - stage_info.start_time
            records_processed = stage_info.processed_records
            total_records = stage_info.total_records
            progress = f"{records_processed}/{total_records}" if total_records else str(records_processed)
            percentage = f"({(records_processed / total_records * 100):.1f}%)" if total_records else ""
            rate = (records_processed / elapsed_time) if elapsed_time > 0 else 0

            # Determine color: Green for periodic logs, Yellow for status updates
            if update and stage_info.status == "Completed":
                color_start = "\033[93m"  # Yellow for completed status update
            else:
                color_start = "\033[92m"  # Green for periodic updates
            
            color_end = "\033[0m"

            log_message = (
                f"[{stage_info.stage_type} Stage: {stage_info.description}] Records Processed: {progress} {percentage} | "
                f"Status: {stage_info.status} | Elapsed Time: {elapsed_time:.2f}s | Rate: {rate:.2f}/s"
            )
            log_messages.append(f"{color_start}{log_message}{color_end}")

        if log_messages:
            self._log(logging.INFO, "\n".join(log_messages))

    def _log(self, log_level: int, log_message: str) -> None:
        if isinstance(self.logger, logging.Logger):
            self.logger.log(log_level, log_message)
        else:
            print(log_message)

    def _categorize_stage(self, stage_name: str) -> str:
        """Categorizes the stage based on known keywords, handling compound names properly."""
        stage_name_clean = stage_name.lower().strip()

        # Remove trailing timestamps or other irrelevant text patterns
        stage_name_clean = re.sub(r"\s*in\s*\d+\.\d+$", "", stage_name_clean)

        # Known stage categories
        known_stages = {
            "extract": "Extract",
            "normalize": "Normalize",
            "load": "Load",
            "resources": "Resources",
            "files": "Files",
            "jobs": "Jobs",
            "tables": "Tables"
        }

        # Match known stages, even if they appear in compound names
        for keyword, category in known_stages.items():
            if re.search(rf"\b{keyword}\b", stage_name_clean):
                return category

        # If no known category is found, classify as "Tables" (default)
        return "Tables"



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