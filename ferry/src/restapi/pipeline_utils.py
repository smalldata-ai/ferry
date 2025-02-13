import logging

from fastapi import HTTPException, status

from urllib.parse import urlparse

import dlt
from dlt.sources.sql_database import sql_database
from dlt.sources.credentials import ConnectionStringCredentials

from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse

logger = logging.getLogger(__name__)


def validate_uri(uri: str) -> bool:
    """Validates a database URI format"""
    parsed = urlparse(uri)
    return all([parsed.scheme, parsed.hostname])


def create_credentials(uri: str) -> ConnectionStringCredentials:
    """Creates DLT credentials from a URI"""
    if not validate_uri(uri):
        raise ValueError(f"Invalid URI format: {uri}")
    return ConnectionStringCredentials(uri)


def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
    """Creates a DLT pipeline"""
    try:
        credentials = create_credentials(destination_uri)
        destination = dlt.destinations.clickhouse(credentials)  # type: ignore
        return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)
    except Exception as e:
        logger.exception(f"Failed to create pipeline: {e}")
        raise RuntimeError(f"Pipeline creation failed: {str(e)}")


@dlt.source
def postgres_source(source_uri: str, table_name: str):
    """Defines a DLT source for PostgreSQL"""
    try:
        credentials = create_credentials(source_uri)
        source = sql_database(credentials)  # type: ignore
        return source.with_resources(table_name)
    except Exception as e:
        logger.exception(f"Error creating source from PostgreSQL: {e}")
        raise RuntimeError(f"Source creation failed: {str(e)}")


async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
    """Handles the data loading process"""
    try:
        pipeline = create_pipeline("postgres_to_clickhouse", request.destination_uri, request.dataset_name)
        source = postgres_source(request.source_uri, request.source_table_name)  # type: ignore

        pipeline.run(source, write_disposition="replace")

        return LoadDataResponse(
            status="success",
            message="Data loaded successfully",
            pipeline_name=pipeline.pipeline_name,
            table_processed=request.destination_table_name,
        )

    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    except RuntimeError as e:
        logger.error(f"Runtime error: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A runtime error occurred")

    except Exception as e:
        logger.exception(f"Unexpected error in load_data_endpoint: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal server error occurred")