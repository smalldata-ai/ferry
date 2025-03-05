
import logging
from fastapi import HTTPException
import dlt

from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory
from ferry.src.restapi.models import (
    LoadDataRequest, LoadDataResponse, LoadStatus, MergeStrategy
)


logger = logging.getLogger(__name__)

def debug_uris(request):
    """Logs source and destination URIs for debugging."""
    logger.info(f"Received source_uri: {request.source_uri}")
    logger.info(f"Received destination_uri: {request.destination_uri}")

def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
    """Initializes the DLT pipeline dynamically based on destination"""
    try:
        # Retrieve the destination dynamically using DestinationFactory
        destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)
        
        # The destination retrieval logic handles all types dynamically
        return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)
        
    except Exception as e:
        logger.exception(f"Failed to create pipeline: {e}")
        raise RuntimeError(f"Pipeline creation failed: {str(e)}")


async def full_load_endpoint(request: LoadDataRequest) -> LoadDataResponse:
    """Load the complete data with 'replace' write disposition"""
    try:
        pipeline = create_pipeline("full_load_pipeline", request.destination_uri, request.dataset_name)
        source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)
        pipeline.run(source, write_disposition=request.write_disposition.value, table_name= request.destination_table_name)
        return LoadDataResponse(
            status=LoadStatus.SUCCESS,
            message="Data loaded successfully with 'replace' write disposition",
            pipeline_name=pipeline.pipeline_name,
            table_processed=request.destination_table_name,
        )
    except Exception as e:
        logger.exception(f"Unexpected error in full load: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal server error occurred")
    
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
            status=LoadStatus.SUCCESS,
            message="Data loaded successfully with 'replace' write disposition",
            pipeline_name=pipeline.pipeline_name,
            table_processed=request.destination_table_name,
        )
    except Exception as e:
        logger.exception(f"Unexpected error in full load: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal server error occurred")


async def merge_load_endpoint(request: LoadDataRequest) -> LoadDataResponse:
    """Merge new data with 'delete-insert', 'scd2', or 'upsert' strategy"""
    try:
        pipeline = create_pipeline("merge_data_pipeline", request.destination_uri, request.dataset_name)
        source = SourceFactory.get(request.source_uri)

        if not request.merge_incremental_load_config:
            raise ValueError("merge_incremental_config is required for MERGE write disposition")
        
        merge_strategy = request.merge_incremental_load_config.merge_strategy
        if merge_strategy == MergeStrategy.DELETE_INSERT:
            source = source.dlt_merge_resource_system(
                request.source_uri,
                request.source_table_name,
                merge_strategy,
                request.merge_incremental_load_config.delete_insert_config
            )
        elif merge_strategy == MergeStrategy.SCD2:
            source = source.dlt_merge_resource_system(
                request.source_uri,
                request.source_table_name,
                merge_strategy,
                request.merge_incremental_load_config.scd2_config
            )
        elif merge_strategy == MergeStrategy.UPSERT:
            source = source.dlt_merge_resource_system(
                request.source_uri,
                request.source_table_name,
                merge_strategy,
                request.merge_incremental_load_config.upsert_config
            )
        else:
            raise ValueError(f"Unsupported merge strategy: {merge_strategy}")
        
        pipeline.run(source, table_name= request.destination_table_name)

        return LoadDataResponse(
            status=LoadStatus.SUCCESS,
            message="Data loaded successfully with 'merge' write disposition",
            pipeline_name=pipeline.pipeline_name,
            table_processed=request.destination_table_name,
        )
    
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error in merge_load_endpoint: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal server error occurred")
        logger.exception(f"Unexpected error in load_data_endpoint: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal server error occurred")

    except Exception as e:
        logger.exception(f"❌ Unexpected error in load_data_endpoint: {e}")
        raise HTTPException(status_code=500, detail="An internal server error occurred")
