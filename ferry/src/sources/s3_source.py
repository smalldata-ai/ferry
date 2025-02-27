import logging
import boto3
import dlt
import io
import pandas as pd
from urllib.parse import urlparse, parse_qs

class S3Source:
    def __init__(self, uri: str, aws_access_key_id=None, aws_secret_access_key=None, aws_region=None, bucket_name=None, chunk_size=100):
        self.uri = uri
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region = aws_region
        self.bucket_name = bucket_name
        self.chunk_size = chunk_size  # Set default chunk size

        # Initialize boto3 client
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
        )

    def read_s3_file(self, uri):
        """Read file in chunks from S3"""
        logger = logging.getLogger(__name__)

        # Parse the URI
        parsed_uri = urlparse(uri)
        bucket_name = self.bucket_name or parsed_uri.netloc
        query_params = parse_qs(parsed_uri.query)
        file_key = query_params.get("file_key", [None])[0] or parsed_uri.path.lstrip("/")

        if not file_key:
            raise ValueError("S3 file key (object path) is missing in the source URI.")

        logger.info(f"Fetching file {file_key} from bucket {bucket_name}")

        # Fetch the file from S3
        response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_data = response["Body"]

        # Detect file format and read in chunks
        if file_key.endswith(".csv"):
            reader = pd.read_csv(file_data, chunksize=self.chunk_size)
        elif file_key.endswith(".json"):
            reader = pd.read_json(file_data, lines=True, chunksize=self.chunk_size)
        elif file_key.endswith(".parquet"):
            reader = pd.read_parquet(io.BytesIO(file_data.read()), chunksize=self.chunk_size)
        else:
            raise ValueError("Unsupported file format. Only CSV, JSON, and Parquet are supported.")

        total_rows = 0
        for chunk in reader:
            chunk_size = len(chunk)
            total_rows += chunk_size
            logger.info(f"Processing {chunk_size} rows")
            yield from chunk.to_dict(orient="records")  # Yield row-wise
        
        logger.info(f"Extracted total {total_rows} rows from S3 file {file_key}")
