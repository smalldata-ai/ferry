import dlt
import boto3
import pandas as pd
import io
from ferry.src.sources.source_base import SourceBase

class S3Source(SourceBase):
    def __init__(self):
        """Initialize S3 connection (like a DB connection)."""
        self.s3_client = boto3.client("s3")

    def dlt_source_system(self, uri: str, bucket_name: str, object_key: str, **kwargs):  # type: ignore
        # Check if the object exists
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        except self.s3_client.exceptions.NoSuchKey:
            raise ValueError(f"⚠️ Object '{object_key}' not found in S3 bucket '{bucket_name}'!")

        # Read data into DataFrame
        file_content = response["Body"].read()
        df = pd.read_csv(io.BytesIO(file_content))

        # Convert to dlt source format
        def generator():
            for record in df.to_dict(orient="records"):
                yield record

        return dlt.resource(generator(), name=object_key)
