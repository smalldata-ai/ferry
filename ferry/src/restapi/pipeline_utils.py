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
import gzip
import zipfile
from urllib.parse import urlparse
from ferry.src.sources.s3_source import S3Source  # Ensure correct import

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class PipelineUtils:
    def __init__(self, db_uri: str):
        """Initialize the pipeline utility with a PostgreSQL connection URI."""
        self.db_uri = db_uri

    def insert_data(self, df: pd.DataFrame, table_name: str) -> int:
        """Bulk insert data into PostgreSQL using COPY command and return the inserted row count."""
        if df.empty:
            logger.warning(f"No data to insert into PostgreSQL for table: {table_name}.")
            return 0

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

            inserted_rows = len(df)
            logger.info(f"Inserted {inserted_rows} rows into table: {table_name}.")
            return inserted_rows

        except psycopg2.Error as e:
            logger.error(f"PostgreSQL error for table {table_name}: {e}")
            raise RuntimeError(f"Database error: {e}")
        finally:
            cursor.close()
            conn.close()

    def read_file(self, file_key, file_stream, chunksize):
        """Read file based on format (CSV, JSON, Parquet, GZIP, ZIP)."""
        if file_key.endswith(".gz"):
            with gzip.GzipFile(fileobj=file_stream) as f:
                return pd.read_csv(f, chunksize=chunksize)
        elif file_key.endswith(".zip"):
            with zipfile.ZipFile(file_stream) as z:
                with z.open(z.namelist()[0]) as f:
                    return pd.read_csv(f, chunksize=chunksize)
        elif file_key.endswith(".csv"):
            return pd.read_csv(file_stream, chunksize=chunksize)
        elif file_key.endswith(".json"):
            return pd.read_json(file_stream, lines=True, chunksize=chunksize)
        elif file_key.endswith(".parquet"):
            df = pd.read_parquet(file_stream)
            return [df[i:i + chunksize] for i in range(0, len(df), chunksize)]
        else:
            raise ValueError("Unsupported file format. Only CSV, JSON, Parquet, GZIP, ZIP are supported.")

    def process_s3_data(self, s3_uri: str, table_name: str, chunksize: int = 1000) -> int:
        """Fetch data from S3 in chunks and insert into PostgreSQL, returning total inserted row count."""
        try:
            s3_source = S3Source(uri=s3_uri)
            parsed_uri = urlparse(s3_uri)
            bucket_name = parsed_uri.netloc
            file_key = parsed_uri.path.lstrip("/")

            # Fetch the file from S3
            response = s3_source.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_stream = io.BytesIO(response["Body"].read())

            # Read file in chunks
            chunk_iterator = self.read_file(file_key, file_stream, chunksize)

            total_inserted = 0
            for chunk in chunk_iterator:
                logger.info(f"Processing chunk with {len(chunk)} rows for table: {table_name}")
                inserted = self.insert_data(chunk, table_name)
                total_inserted += inserted

            logger.info(f"Completed processing for table {table_name} from {s3_uri}. Total rows inserted: {total_inserted}")
            return total_inserted

        except Exception as e:
            logger.error(f"Error processing S3 data for table {table_name}: {e}")
            raise RuntimeError(f"S3 processing error: {e}")
