import logging
import boto3
import dlt
from urllib.parse import urlparse, parse_qs
from ferry.src.exceptions import InvalidSourceException
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator  # Import the validator

class S3Source:
    def __init__(self, uri: str):
        self.logger = logging.getLogger(__name__)  # Define logger inside the class
        self.logger.debug(f"Initializing S3Source with URI: {uri}")

        # Validate the S3 URI using DatabaseURIValidator
        self.validate_uri(uri)  

        self.uri = uri
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)

        # Extract bucket name from netloc
        self.bucket_name = parsed_uri.netloc

        # Extract file key (object path)
        self.file_key = query_params.get("file_key", [None])[0] or parsed_uri.path.lstrip("/")

        # Log parsed S3 details
        self.logger.info(f"Parsed S3 details -> Bucket: {self.bucket_name}, File Key: {self.file_key}")

        # Initialize S3 client
        self.aws_access_key_id = query_params.get("access_key_id", [None])[0]
        self.aws_secret_access_key = query_params.get("access_key_secret", [None])[0]
        self.aws_region = query_params.get("region", [None])[0]

        if self.aws_access_key_id and self.aws_secret_access_key:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region,
            )
        else:
            self.s3_client = boto3.client("s3", region_name=self.aws_region)

    def validate_uri(self, uri: str):
        """Validate that the S3 URI is well-formed using DatabaseURIValidator."""
        try:
            DatabaseURIValidator.validate_s3(uri)
            self.logger.debug("S3 URI validation passed.")
        except ValueError as e:
            self.logger.error(f"S3 URI validation failed: {e}")
            raise InvalidSourceException(f"Invalid S3 URI: {e}")

    def dlt_source_system(self, uri, source_table_name):
        """Convert S3 file into a DLT resource"""
        return dlt.resource(self.read_s3_file(), name=source_table_name)

    def read_s3_file(self):
        """Read file from S3 and yield data"""
        import io
        import pandas as pd

        self.logger.info(f"Fetching file {self.file_key} from bucket {self.bucket_name}")

        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_key)
        file_data = response["Body"].read()

        df = pd.read_csv(io.BytesIO(file_data))
        self.logger.info(f"Extracted {len(df)} rows from S3 file {self.file_key}")

        for record in df.to_dict(orient="records"):
            yield record
