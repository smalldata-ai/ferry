# # # import logging

# # # from fastapi import HTTPException, status

# # # import dlt

# # # from ferry.src.destination_factory import DestinationFactory
# # # from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
# # # from ferry.src.source_factory import SourceFactory

# # # logger = logging.getLogger(__name__)


# # # def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
# # #     """Initializes the DLT pipeline"""
# # #     try:
# # #         destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)
# # #         return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)
# # #     except Exception as e:
# # #         logger.exception(f"Failed to create pipeline: {e}")
# # #         raise RuntimeError(f"Pipeline creation failed: {str(e)}")


# # # async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
# # #     """Triggers the Extraction, Normalization and Loading of data from source to destination"""
# # #     try:
# # #         pipeline = create_pipeline("postgres_to_clickhouse", request.destination_uri, request.dataset_name)
        
# # #         source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)

# # #         pipeline.run(source, write_disposition="replace")

# # #         return LoadDataResponse(
# # #             status="success",
# # #             message="Data loaded successfully",
# # #             pipeline_name=pipeline.pipeline_name,
# # #             table_processed=request.destination_table_name,
# # #         )

# # #     except RuntimeError as e:
# # #         logger.error(f"Runtime error: {e}")
# # #         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="A runtime error occurred")

# # #     except Exception as e:
# # #         logger.exception(f"Unexpected error in load_data_endpoint: {e}")
# # #         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal server error occurred")

# # import logging

# # from fastapi import HTTPException, status

# # import dlt

# # from ferry.src.destination_factory import DestinationFactory
# # from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
# # from ferry.src.source_factory import SourceFactory

# # logger = logging.getLogger(__name__)

# # def debug_uris(request):
# #     logger.info(f"Received source_uri: {request.source_uri}")
# #     logger.info(f"Received destination_uri: {request.destination_uri}")

# # def create_pipeline(pipeline_name: str, destination_uri: str, dataset_name: str) -> dlt.Pipeline:
# #     """Initializes the DLT pipeline dynamically based on destination"""
# #     try:
# #         destination = DestinationFactory.get(destination_uri).dlt_target_system(destination_uri)

# #         # Ensure DuckDB is passed correctly
# #         if destination_uri.startswith("duckdb"):
# #             destination = "duckdb"  # Override if DuckDB URI is given

# #         return dlt.pipeline(pipeline_name=pipeline_name, destination=destination, dataset_name=dataset_name)
# #     except Exception as e:
# #         logger.exception(f"Failed to create pipeline: {e}")
# #         raise RuntimeError(f"Pipeline creation failed: {str(e)}")



# # async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
# #     """Triggers the Extraction, Normalization, and Loading of data from source to destination"""
# #     try:
# #         debug_uris(request)

# #         # Dynamically determine the pipeline name based on source and destination
# #         source_scheme = request.source_uri.split(":")[0]
# #         destination_scheme = request.destination_uri.split(":")[0]
# #         pipeline_name = f"{source_scheme}_to_{destination_scheme}"

# #         # Create the pipeline
# #         pipeline = create_pipeline(pipeline_name, request.destination_uri, request.dataset_name)
        
# #         # Fetch the source system
# #         source = SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)

# #         # Run the pipeline
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

# from urllib.parse import urlparse, parse_qs
# import logging
# from fastapi import HTTPException, status
# import dlt

# from ferry.src.destination_factory import DestinationFactory
# from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
# from ferry.src.source_factory import SourceFactory

# logger = logging.getLogger(__name__)

# def debug_uris(request):
#     """Logs source and destination URIs for debugging."""
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
    
# def create_s3_source(request):
#     """Creates a fresh S3 source instance every time pipeline.run() is called."""
#     return SourceFactory.get(request.source_uri).dlt_source_system(request.source_uri, request.source_table_name)


# async def load_data_endpoint(request: LoadDataRequest) -> LoadDataResponse:
#     """Triggers the Extraction, Normalization, and Loading of data from source to destination"""
#     try:
#         debug_uris(request)

#         # Determine the pipeline name dynamically
#         if request.destination_uri.startswith("duckdb:///"):
#             pipeline_name = request.destination_uri.split("duckdb:///")[-1]  # Extract file name
#             pipeline_name = pipeline_name.replace("/", "_").replace(".", "_")  # Sanitize for pipeline naming
#         else:
#             source_scheme = request.source_uri.split(":")[0]
#             destination_scheme = request.destination_uri.split(":")[0]
#             pipeline_name = f"{source_scheme}_to_{destination_scheme}"

#         logger.info(f"Pipeline name resolved as: {pipeline_name}")

#         # Create the pipeline
#         pipeline = create_pipeline(pipeline_name, request.destination_uri, request.dataset_name)

#         # Ensure a fresh S3 source instance is created
#         if request.source_uri.startswith("s3://"):
#             logger.info(f"Initializing new S3 source for {request.source_uri}")
#             source_instance = SourceFactory.get(request.source_uri)
#             source = source_instance.dlt_source_system(request.source_uri, request.source_table_name)
#         else:
#             logger.info(f"Initializing source for {request.source_uri}")
#             source_instance = SourceFactory.get(request.source_uri)
#             source = source_instance.dlt_source_system(request.source_uri, request.source_table_name)

#         # Run the pipeline
#         logger.info(f"Running pipeline for table: {request.destination_table_name}")
#         # pipeline.run(
#         #     source, 
#         #     table_name="table_stroke", 
#         #     schema="my_dataset", 
#         #     write_disposition="replace"  # Ensures fresh table creation
#         # )

#         pipeline.run(source, table_name=request.destination_table_name, write_disposition="replace")
#         # pipeline.run(source, table_name=request.destination_table_name, schema="public", write_disposition="append", create_disposition="create_if_missing")
#         # pipeline.run(source, table_name=request.destination_table_name, schema="public", write_disposition="append")
#         # pipeline.run(source, table_name=request.destination_table_name, schema="public", write_disposition="append")
#         # pipeline.run(source, table_name="table_stroke", schema="my_dataset", write_disposition="append")

#         # source = source_instance.dlt_source_system(request.source_uri, request.source_table_name)
#         # pipeline.run(source)  # ✅ Use it only once
        
#         logger.info("Pipeline run completed successfully for table %s", request.destination_table_name)
#         print(f"Loaded data into {request.destination_table_name} in {request.destination_uri}")

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


##new approach

import logging
import psycopg2
import io
import pandas as pd
from ferry.src.sources.s3_source import S3Source  # Ensure correct import based on your structure

# PostgreSQL connection details
DB_URI = "postgresql://username:password@localhost:5432/real_estate_data"

class PipelineUtils:
    def __init__(self, db_uri=DB_URI):
        """Initialize the pipeline utility with PostgreSQL connection URI."""
        self.db_uri = db_uri

    def insert_data(self, df: pd.DataFrame, table_name="real_estate"):
        """Bulk insert data into PostgreSQL using COPY command for efficiency."""
        if df.empty:
            logging.warning("No data to insert into PostgreSQL.")
            return

        try:
            conn = psycopg2.connect(self.db_uri)
            cursor = conn.cursor()
            buffer = io.StringIO()

            # Convert DataFrame to CSV format for bulk insertion
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            columns = ", ".join(df.columns)
            copy_query = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"

            cursor.copy_expert(copy_query, buffer)
            conn.commit()

            logging.info(f"Inserted {len(df)} rows into {table_name}.")
        except Exception as e:
            logging.error(f"Error inserting data into PostgreSQL: {e}")
        finally:
            cursor.close()
            conn.close()

    def process_s3_data(self, s3_uri, table_name="real_estate"):
        """Fetch data from S3 and insert it into PostgreSQL."""
        s3_source = S3Source(uri=s3_uri)
        data = list(s3_source.read_s3_file(s3_uri))  # Convert generator to list

        if data:
            df = pd.DataFrame(data)
            self.insert_data(df, table_name)
        else:
            logging.warning(f"No data extracted from S3 file: {s3_uri}")

