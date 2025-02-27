import dlt
from urllib.parse import urlparse
import boto3
import urllib.parse
from ferry.src.sources.source_base import SourceBase
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator
from ferry.src.exceptions import InvalidSourceException

class S3Source(SourceBase):

    def __init__(self, uri: str):
        """Initialize S3 source and validate the URI."""
        self.validate_uri(uri)
        self.uri = uri

    def validate_uri(self, uri: str):
        """Use centralized URI validation for S3."""
        parsed = urlparse(uri)
        print(f"Parsed URI: {parsed}") 
        try:
            DatabaseURIValidator.validate_uri(uri)
        except ValueError as e:
            raise InvalidSourceException(f"Invalid S3 URI: {e}")

    def dlt_source_system(self, uri: str, table_name: str):
        """Fetch data from S3 and create a dlt resource."""
        
        # Parse the URI
        parsed_uri = urllib.parse.urlparse(uri)
        bucket_name = parsed_uri.netloc  # Bucket is in netloc
        query_params = urllib.parse.parse_qs(parsed_uri.query)

        # Extract credentials and file key
        access_key = query_params.get("access_key_id", [None])[0]
        secret_key = query_params.get("access_key_secret", [None])[0]
        region = query_params.get("region", [None])[0]
        file_key = query_params.get("file_key", [None])[0]

        if not all([bucket_name, file_key, access_key, secret_key, region]):
            raise ValueError("Missing required S3 parameters")

        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )

        # Download the file
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = obj["Body"].read().decode("utf-8")

        # Convert to DLT resource
        def data_generator():
            for line in file_content.splitlines():
                yield line.split(",")  # Adjust based on file format

        return dlt.resource(data_generator(), name=table_name)
