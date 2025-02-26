# # import logging

# # from fastapi import HTTPException, status

# # import dlt

# # from ferry.src.destination_factory import DestinationFactory
# # from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
# # from ferry.src.source_factory import SourceFactory

# # logger = logging.getLogger(__name__)


# # def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
# #     """Initializes the DLT pipeline"""
# #     try:
# #         destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)
# #         return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)
# #     except Exception as e:
# #         logger.exception(f"Failed to create pipeline: {e}")
# #         raise RuntimeError(f"Pipeline creation failed: {str(e)}")


# # async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
# #     """Triggers the Extraction, Normalization and Loading of data from source to destination"""
# #     try:
# #         pipeline = create_pipeline("postgres_to_clickhouse", request.destination_uri, request.dataset_name)
        
# #         source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)

# #         pipeline.run(source, write_disposition="replace")

# #         return LoadDataResponse(
# #             status="success",
# #             message="Data loaded successfully",
# #             pipeline_name=pipeline.pipeline_name,
# #             table_processed=request.destination_table_name,
# #         )

# #     except RuntimeError as e:
# #         logger.error(f"Runtime error: {e}")
# #         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A runtime error occurred")

# #     except Exception as e:
# #         logger.exception(f"Unexpected error in load_data_endpoint: {e}")
# #         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal server error occurred")

# import logging

# from fastapi import HTTPException, status

# import dlt

# from ferry.src.destination_factory import DestinationFactory
# from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
# from ferry.src.source_factory import SourceFactory

# logger = logging.getLogger(__name__)

# def debug_uris(request):
#     logger.info(f"Received source_uri: {request.source_uri}")
#     logger.info(f"Received destination_uri: {request.destination_uri}")

# def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
#     """Initializes the DLT pipeline dynamically based on destination"""
#     try:
#         destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)

#         # Ensure DuckDB is passed correctly
#         if destination_uri.startswith("duckdb"):
#             destination = "duckdb"  # Override if DuckDB URI is given

#         return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)
#     except Exception as e:
#         logger.exception(f"Failed to create pipeline: {e}")
#         raise RuntimeError(f"Pipeline creation failed: {str(e)}")



# async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
#     """Triggers the Extraction, Normalization, and Loading of data from source to destination"""
#     try:
#         debug_uris(request)

#         # Dynamically determine the pipeline name based on source and destination
#         source_scheme = request.source_uri.split(":")[0]
#         destination_scheme = request.destination_uri.split(":")[0]
#         pipeline_name = f"{source_scheme}_to_{destination_scheme}"

#         # Create the pipeline
#         pipeline = create_pipeline(pipeline_name, request.destination_uri, request.dataset_name)
        
#         # Fetch the source system
#         source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)

#         # Run the pipeline
#         pipeline.run(source, write_disposition="replace")

#         return LoadDataResponse(
#             status="success",
#             message="Data loaded successfully",
#             pipeline_name=pipeline.pipeline_name,
#             table_processed=request.destination_table_name,
#         )

#     except RuntimeError as e:
#         logger.error(f"Runtime error: {e}")
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A runtime error occurred")

#     except Exception as e:
#         logger.exception(f"Unexpected error in load_data_endpoint: {e}")
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal server error occurred")

import logging
from fastapi import HTTPException, status
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

# def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
#     """Initializes the DLT pipeline dynamically based on destination"""
#     try:
#         destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)

#         return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)
#     except Exception as e:
#         logger.exception(f"Failed to create pipeline: {e}")
#         raise RuntimeError(f"Pipeline creation failed: {str(e)}")

def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
    """Initializes the DLT pipeline dynamically based on destination"""
    try:
        destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)

        # Ensure DuckDB is passed correctly
        if destination_uri.startswith("duckdb"):
            destination = "duckdb"  # Override if DuckDB URI is given

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
        debug_uris(request)

        # Fix: Ensure DuckDB pipeline name uses file name instead of "duckdb_to_duckdb"
        if request.destination_uri.startswith("duckdb:///"):
            pipeline_name = request.destination_uri.split("duckdb:///")[-1]  # Extract file name
            pipeline_name = pipeline_name.replace("/", "_").replace(".", "_")  # Sanitize for pipeline naming
        else:
            source_scheme = request.source_uri.split(":")[0]
            destination_scheme = request.destination_uri.split(":")[0]
            pipeline_name = f"{source_scheme}_to_{destination_scheme}"

        logger.info(f"Pipeline name resolved as: {pipeline_name}")

        # Create the pipeline with correct naming
        pipeline = create_pipeline(pipeline_name, request.destination_uri, request.dataset_name)

        # Fetch the source system
        source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)

        # Run the pipeline
        # pipeline.run(source, write_disposition="replace")
        # Run the pipeline with explicit destination table name
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
