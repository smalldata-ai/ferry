import dlt
import boto3
import pandas as pd
import io
from ferry.src.sources.source_base import SourceBase

class S3Source(SourceBase):
    def __init__(self, aws_access_key=None, aws_secret_key=None, region="us-east-1"):
        """Initialize S3 connection with optional credentials."""
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):  # type: ignore
        """Fetches data from S3 and converts it into a format usable by the DLT pipeline."""
        s3_uri_parts = uri.replace("s3://", "").split("/", 1)
        if len(s3_uri_parts) != 2:
            raise ValueError(f"❌ Invalid S3 URI format: {uri}. Expected 's3://bucket-name/path/to/file.csv'.")

        bucket_name, object_key = s3_uri_parts

        # Check if bucket exists
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
        except self.s3_client.exceptions.NoSuchBucket:
            raise ValueError(f"⚠️ S3 Bucket '{bucket_name}' not found!")

        # Check if object exists
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        except self.s3_client.exceptions.NoSuchKey:
            raise ValueError(f"⚠️ Object '{object_key}' not found in S3 bucket '{bucket_name}'!")

        # Read CSV data
        file_content = response["Body"].read()
        df = pd.read_csv(io.BytesIO(file_content))

        # Convert DataFrame to DLT-compatible format
        def generator():
            for record in df.to_dict(orient="records"):
                yield record

        return dlt.resource(generator(), name=table_name)
