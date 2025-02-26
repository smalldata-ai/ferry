import logging
import boto3
import dlt

class S3Source:
    def __init__(self, uri: str, aws_access_key_id=None, aws_secret_access_key=None, aws_region=None, bucket_name=None):
        self.uri = uri
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region = aws_region
        self.bucket_name = bucket_name

        # Initialize boto3 client with credentials (if provided)
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
        )

    def dlt_source_system(self, uri, source_table_name):
        """Convert S3 file into a DLT resource"""
        return dlt.resource(self.read_s3_file(uri), name=source_table_name)


    def read_s3_file(self, uri):
        """Read file from S3 and yield data"""
        import io
        import pandas as pd
        from urllib.parse import urlparse, parse_qs

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
        file_data = response["Body"].read()

        df = pd.read_csv(io.BytesIO(file_data))
        logger.info(f"Extracted {len(df)} rows from S3 file {file_key}")

        for record in df.to_dict(orient="records"):
            yield record
