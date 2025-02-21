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
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.source_factory import SourceFactory

logger = logging.getLogger(__name__)

def debug_uris(request):
    """Logs source and destination URIs for debugging."""
    logger.info(f"Received source_uri: {request.source_uri}")
    logger.info(f"Received destination_uri: {request.destination_uri}")

def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
    """Initializes the DLT pipeline dynamically based on the destination system."""
    try:
        destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)

        # Ensure DuckDB is configured correctly
        if destination_uri.startswith("duckdb://"):
            duckdb_path = destination_uri.replace("duckdb:///", "")
            destination = dlt.destinations.duckdb(configuration={"database": duckdb_path})

        return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)

    except Exception as e:
        logger.exception(f"Failed to create pipeline: {e}")
        raise RuntimeError(f"Pipeline creation failed: {str(e)}")

async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
    """Triggers Extraction, Normalization, and Loading of data from source to destination."""
    try:
        debug_uris(request)

        # Dynamically determine the pipeline name based on source and destination
        source_scheme = request.source_uri.split(":")[0]
        destination_scheme = request.destination_uri.split(":")[0]
        pipeline_name = f"{source_scheme}_to_{destination_scheme}"

        # Create the pipeline
        pipeline = create_pipeline(pipeline_name, request.destination_uri, request.dataset_name)
        
        # Fetch the source system
        source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)

        # Run the pipeline
        pipeline.run(source, write_disposition="replace")

        return LoadDataResponse(
            status="success",
            message="Data loaded successfully",
            pipeline_name=pipeline.pipeline_name,
            table_processed=request.destination_table_name,
        )

    except RuntimeError as e:
        logger.error(f"Runtime error: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A runtime error occurred")

    except Exception as e:
        logger.exception(f"Unexpected error in load_data_endpoint: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal server error occurred")

