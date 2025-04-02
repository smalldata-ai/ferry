import dlt
import urllib.parse
import logging
from typing import List, Optional
from dlt.common.configuration.specs import AwsCredentials
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from dlt.extract.source import DltSource
from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig

logger = logging.getLogger(__name__)

class S3Source(SourceBase):
    def __init__(self):
        super().__init__()

    def dlt_source_system(self, uri: str, resources: List[ResourceConfig], identity: str) -> DltSource:
        """Fetch multiple tables from S3 and create a DLT source with resources."""
        bucket_name, aws_credentials = self._parse_s3_uri(uri)
        resources_list = []

        for resource_config in resources:
            table_name = resource_config.source_table_name
            logger.info(f"Processing table: {table_name}")

            # Select reader based on file extension
            reader = self._get_reader(table_name)

            # Determine the incremental loading strategy
            file_incremental = dlt.sources.incremental("modification_date")  # Track file modifications
            row_incremental = self._get_row_incremental(resource_config)  # Track new records inside files

            # Create file resource and apply incremental loading hints
            file_resource = filesystem(f"s3://{bucket_name}", aws_credentials, f"{table_name}*")
            file_resource.apply_hints(incremental=file_incremental)  # Track modified files

            # Apply both file-based and row-based incremental loading
            data_iterator = file_resource | reader()
            if row_incremental:
                data_iterator = dlt.sources.incremental(row_incremental)(data_iterator)

            resources_list.append(self._create_dlt_resource(resource_config, data_iterator))

        return DltSource(
            schema=dlt.Schema(identity),
            section="s3_source",
            resources=resources_list
        )

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

    def _get_reader(self, table_name: str):
        """Returns the appropriate reader function based on file format."""
        lower_table_name = table_name.lower()
        if lower_table_name.endswith(".csv"):
            return read_csv
        elif lower_table_name.endswith(".jsonl"):
            return read_jsonl
        elif lower_table_name.endswith(".parquet"):
            return read_parquet
        else:
            raise ValueError(f"Unsupported file format for table: {table_name}")

    def _get_row_incremental(self, resource_config: ResourceConfig) -> Optional[str]:
        """Determines the incremental column for row-level filtering."""
        if resource_config.incremental_config:
            incremental_config = resource_config.incremental_config.build_config()
            return incremental_config.get("incremental_key")
        return None
