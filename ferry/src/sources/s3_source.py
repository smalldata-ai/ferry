import dlt
from urllib.parse import urlparse, parse_qs
import boto3
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
        query_params = parse_qs(parsed.query)
        
        # Extract required parameters
        bucket_name = parsed.netloc  # Bucket should be here
        file_key = parsed.path.lstrip("/")  # Remove leading "/"

        print(f"DEBUG - Parsed URI: {parsed}")
        print(f"DEBUG - Query Params: {query_params}")
        print(f"DEBUG - Bucket Name: {bucket_name}")
        print(f"DEBUG - File Key: {file_key}")

        # Ensure required parameters exist
        missing_keys = []
        if not bucket_name:
            missing_keys.append("bucket_name")
        if not file_key:
            missing_keys.append("file_key")

        if missing_keys:
            raise ValueError(f"Missing required S3 parameters: {missing_keys}")

        # Validate URI format using external class
        try:
            DatabaseURIValidator.validate_uri(uri)  # ✅ Pass `uri`
        except ValueError as e:
            raise InvalidSourceException(f"Invalid S3 URI: {e}")


    def dlt_source_system(self, uri: str, table_name: str):
        """Fetch data from S3 and create a dlt resource."""

        # Parse the URI
        parsed_uri = urlparse(uri)
        bucket_name = parsed_uri.netloc  # ✅ Bucket should be in netloc
        file_key = parsed_uri.path.lstrip("/")  # ✅ File key should be in path
        query_params = parse_qs(parsed_uri.query)

        # Extract credentials
        access_key = query_params.get("access_key_id", [None])[0]
        secret_key = query_params.get("access_key_secret", [None])[0]
        region = query_params.get("region", [None])[0]

        # Debugging prints
        print(f"DEBUG - Bucket Name: {bucket_name}")
        print(f"DEBUG - File Key: {file_key}")
        print(f"DEBUG - Access Key: {access_key}")
        print(f"DEBUG - Secret Key: {secret_key}")
        print(f"DEBUG - Region: {region}")

        # Ensure all required parameters exist
        missing_keys = []
        if not bucket_name:
            missing_keys.append("bucket_name")
        if not file_key:
            missing_keys.append("file_key")
        if not access_key:
            missing_keys.append("access_key_id")
        if not secret_key:
            missing_keys.append("access_key_secret")
        if not region:
            missing_keys.append("region")

        if missing_keys:
            raise ValueError(f"Missing required S3 parameters: {missing_keys}")

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
