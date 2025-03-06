import dlt
import urllib.parse
from dlt.common.configuration.specs import AwsCredentials
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from ferry.src.sources.source_base import SourceBase

class S3Source(SourceBase):
    def __init__(self):
        super().__init__()

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        """Fetch data from S3 and create a dlt resource."""
        bucket_name, aws_credentials = self._parse_s3_uri(uri)
        
        file_resource = self._create_file_resource(bucket_name, aws_credentials, table_name)
        return self._apply_reader(file_resource, table_name)
    
    def _parse_s3_uri(self, uri: str):
        """Extracts bucket name and AWS credentials from the URI."""
        parsed_uri = urllib.parse.urlparse(uri)
        bucket_name = parsed_uri.hostname
        query_params = urllib.parse.parse_qs(parsed_uri.query)

        aws_credentials = AwsCredentials(
            aws_access_key_id=query_params.get("access_key_id", [None])[0],
            aws_secret_access_key=query_params.get("access_key_secret", [None])[0],
            region_name=query_params.get("region", [None])[0]
        )
        return bucket_name, aws_credentials

    def _create_file_resource(self, bucket_name: str, aws_credentials: AwsCredentials, table_name: str):
        """Creates a dlt file resource with incremental loading."""
        file_resource = filesystem(f"s3://{bucket_name}", aws_credentials, f"{table_name}*")
        file_resource.apply_hints(incremental=dlt.sources.incremental("modification_date"))
        return file_resource

    def _apply_reader(self, file_resource, table_name: str):
        """Applies the appropriate reader based on file extension."""
        lower_table_name = table_name.lower()
        if lower_table_name.endswith(".csv"):
            return file_resource | read_csv()
        elif lower_table_name.endswith(".jsonl"):
            return file_resource | read_jsonl()
        elif lower_table_name.endswith(".parquet"):
            return file_resource | read_parquet()
        else:
            raise ValueError(f"Unsupported file format for table: {table_name}")