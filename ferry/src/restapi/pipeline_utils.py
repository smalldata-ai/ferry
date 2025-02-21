import logging
from fastapi import HTTPException, status
import dlt

from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory
from ferry.src.restapi.models import (
    LoadDataRequest, LoadDataResponse, LoadStatus, MergeStrategy
)


logger = logging.getLogger(__name__)


def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
    """Initialize the DLT pipeline"""
    try:
        destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)
        return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)
    except Exception as e:
        logger.exception(f"Failed to create pipeline: {e}")
        raise RuntimeError(f"Pipeline creation failed: {str(e)}")


async def full_load_endpoint(request: LoadDataRequest) -> LoadDataResponse:
    """Load the complete data with 'replace' write disposition"""
    try:
        pipeline = create_pipeline("full_load_pipeline", request.destination_uri, request.dataset_name)
        source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)
        pipeline.run(source, write_disposition=request.write_disposition.value)

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
        
        pipeline.run(source)

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