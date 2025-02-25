import boto3
import pandas as pd
import io
import os
from ferry.src.sources.source_base import SourceBase

from ferry.src.sources.s3_source import S3Source

source = S3Source()
print(source.s3_client.list_buckets())  # Should return a list of S3 buckets if credentials are correct

class S3Source(SourceBase):
    def __init__(self, aws_access_key=None, aws_secret_key=None, region="us-east-1"):
        """Initialize S3 connection with credentials from parameters or environment variables."""
        
        # Use environment variables if parameters are not provided
        aws_access_key = aws_access_key or os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = aws_secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        region = region or os.getenv("AWS_REGION", "us-east-1")

        if not aws_access_key or not aws_secret_key:
            raise ValueError("AWS credentials not provided. Set environment variables or pass them as arguments.")

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )


    def fetch_csv_from_s3(self, uri: str):
        """Fetch CSV from S3 and return a Pandas DataFrame."""
        s3_uri_parts = uri.replace("s3://", "").split("/", 1)
        if len(s3_uri_parts) != 2:
            raise ValueError(f"❌ Invalid S3 URI format: {uri}. Expected 's3://bucket-name/path/to/file.csv'.")

        bucket_name, object_key = s3_uri_parts

        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = response["Body"].read()
            df = pd.read_csv(io.BytesIO(file_content))
            return df
        except self.s3_client.exceptions.NoSuchKey:
            raise ValueError(f"⚠️ Object '{object_key}' not found in S3 bucket '{bucket_name}'!")
        except Exception as e:
            raise Exception(f"❌ Error fetching CSV from S3: {str(e)}")
