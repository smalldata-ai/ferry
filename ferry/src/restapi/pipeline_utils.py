import logging
from fastapi import HTTPException, status
import dlt

from ferry.src.destination_factory import DestinationFactory
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.sources.source_factory import SourceFactory

logger = logging.getLogger(__name__)

def debug_uris(request):
    """Logs source and destination URIs for debugging."""
    logger.info(f"Received source_uri: {request.source_uri}")
    logger.info(f"Received destination_uri: {request.destination_uri}")

def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
    """Initializes the DLT pipeline dynamically for S3 to PostgreSQL"""
    try:
        logger.info(f"Creating pipeline: {pipeline_name} for destination: {destination_uri}")

        # Ensure PostgreSQL is properly configured
        if destination_uri.startswith("postgres://"):
            destination = dlt.destinations.postgres(configuration={"database": destination_uri})
        else:
            raise ValueError("Unsupported destination. Only PostgreSQL is supported in this pipeline.")

        return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)

    except Exception as e:
        logger.exception(f"Failed to create pipeline: {e}")
        raise RuntimeError(f"Pipeline creation failed: {str(e)}")

async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
    """Loads CSV data from S3 into PostgreSQL."""
    try:
        debug_uris(request)

        # Extract source and destination schemes
        source_scheme = request.source_uri.split(":")[0]
        destination_scheme = request.destination_uri.split(":")[0]

        if source_scheme != "s3":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only S3 is supported as a source.")

        if destination_scheme != "postgres":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only PostgreSQL is supported as a destination.")

        # Generate pipeline name
        pipeline_name = "s3_to_postgres"

        # Create the pipeline
        pipeline = create_pipeline(pipeline_name, request.destination_uri, request.dataset_name)
        
        # Fetch the S3 source system
        logger.info(f"Fetching S3 source for URI: {request.source_uri}")
        source = dlt.sources.filesystem.s3_csv(request.source_uri, request.source_table_name)

        # Run the pipeline
        logger.info("Running data pipeline from S3 to PostgreSQL...")
        pipeline.run(source, write_disposition="replace")

        logger.info("Data successfully loaded from S3 to PostgreSQL.")

        return LoadDataResponse(
            status="success",
            message="Data loaded successfully from S3 to PostgreSQL.",
            pipeline_name=pipeline.pipeline_name,
            table_processed=request.destination_table_name,
        )

    except HTTPException as e:
        raise e  # Re-raise HTTP exceptions directly

    except RuntimeError as e:
        logger.error(f"Runtime error: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"A runtime error occurred: {str(e)}")

    except Exception as e:
        logger.exception(f"Unexpected error in load_data_endpoint: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal error: {str(e)}")
