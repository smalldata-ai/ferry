import logging
import boto3
import dlt
from urllib.parse import urlparse, parse_qs
from ferry.src.exceptions import InvalidSourceException

class S3Source:
    def __init__(self, uri: str):
        self.uri = uri
        self.s3_client = None  # Initialize S3 client later

        # Validate URI
        self.validate_uri(uri)

        # Parse URI and extract query parameters (credentials, region, bucket, file_key)
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)

        # Extract and set AWS credentials and other parameters
        self.aws_access_key_id = query_params.get("access_key_id", [None])[0]
        self.aws_secret_access_key = query_params.get("access_key_secret", [None])[0]
        self.aws_region = query_params.get("region", [None])[0]
        self.bucket_name = query_params.get("bucket_name", [None])[0]
        self.file_key = query_params.get("file_key", [None])[0] or parsed_uri.path.lstrip("/")

        # Initialize boto3 client with credentials (if provided)
        if self.aws_access_key_id and self.aws_secret_access_key:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region,
            )
        else:
            # If no credentials, use default credentials (if configured)
            self.s3_client = boto3.client("s3", region_name=self.aws_region)

    def validate_uri(self, uri: str):
        """Validate that the S3 URI is well-formed."""
        parsed_uri = urlparse(uri)

        # Ensure the URI scheme is 's3'
        if parsed_uri.scheme != "s3":
            raise InvalidSourceException(f"Invalid scheme '{parsed_uri.scheme}' in URI. Expected 's3'.")

        # Ensure the bucket_name and file_key are provided
        if not self.bucket_name:
            raise InvalidSourceException("Bucket name is missing in the source URI.")
        if not self.file_key:
            raise InvalidSourceException("File key (object path) is missing in the source URI.")

    def dlt_source_system(self, uri, source_table_name):
        """Convert S3 file into a DLT resource"""
        return dlt.resource(self.read_s3_file(uri), name=source_table_name)

    def read_s3_file(self, uri):
        """Read file from S3 and yield data"""
        import io
        import pandas as pd

        logger = logging.getLogger(__name__)

        logger.info(f"Fetching file {self.file_key} from bucket {self.bucket_name}")

        # Fetch the file from S3
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_key)
        file_data = response["Body"].read()

        # Read CSV data into a pandas DataFrame
        df = pd.read_csv(io.BytesIO(file_data))
        logger.info(f"Extracted {len(df)} rows from S3 file {self.file_key}")

        # Convert DataFrame to dict records for dlt.resource
        for record in df.to_dict(orient="records"):
            yield record
