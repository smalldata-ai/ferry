from urllib.parse import urlparse
import dlt
import boto3
from ferry.src.sources.source_base import SourceBase

class S3Source(SourceBase):
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, aws_region: str):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        parsed_uri = urlparse(uri)
        bucket_name = parsed_uri.netloc
        key = parsed_uri.path.lstrip("/")

        obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        data = obj["Body"].read().decode("utf-8").splitlines()

        # Convert CSV to dlt resource
        def generator():
            for row in data:
                yield row.split(",")

        return dlt.resource(generator(), name=table_name)
