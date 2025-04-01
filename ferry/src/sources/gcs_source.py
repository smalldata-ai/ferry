import dlt
import urllib.parse
import logging
from typing import List, Optional
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from dlt.extract.source import DltSource
from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig

logger = logging.getLogger(__name__)

class GCSSource(SourceBase):
    def __init__(self):
        super().__init__()

    def dlt_source_system(self, uri: str, resources: List[ResourceConfig], identity: str) -> DltSource:
        """Fetch multiple tables from GCS and create a DLT source with resources."""
        bucket_name, gcp_credentials = self._parse_gcs_uri(uri)
        resources_list = []

        for resource_config in resources:
            table_name = resource_config.source_table_name
            file_pattern = resource_config.file_pattern or f"{table_name}*"
            logger.info(f"Processing table: {table_name} with file pattern: {file_pattern}")

            # Select reader based on file extension
            reader = self._get_reader(table_name)

            # File-level incremental loading (same as current)
            file_incremental = dlt.sources.incremental("modification_date")  
            row_incremental = self._get_row_incremental(resource_config)  

            # Create file resource and apply incremental loading hints
            file_resource = filesystem(f"gs://{bucket_name}", gcp_credentials, file_pattern)
            file_resource.apply_hints(incremental=file_incremental) 

            # Apply both file-based and row-based incremental loading (if applicable)
            data_iterator = file_resource | reader()
            if row_incremental:
                data_iterator = dlt.sources.incremental(row_incremental)(data_iterator)

            # Append the resource with the appropriate configuration
            resources_list.append(self._create_dlt_resource(resource_config, data_iterator))

        # Return a DLT source system containing all resources
        return DltSource(
            schema=dlt.Schema(identity),
            section="gcs_source",
            resources=resources_list
        )

    def _parse_gcs_uri(self, uri: str):
        """Extracts bucket name and GCP credentials from the URI."""
        parsed_uri = urllib.parse.urlparse(uri)
        bucket_name = parsed_uri.hostname
        query_params = urllib.parse.parse_qs(parsed_uri.query)

        gcp_credentials = GcpServiceAccountCredentials(
            project_id=query_params.get("project_id", [None])[0],
            private_key=query_params.get("private_key", [None])[0],
            client_email=query_params.get("client_email", [None])[0],
        )
        return bucket_name, gcp_credentials

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
