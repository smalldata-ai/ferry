import dlt
import urllib.parse
import logging
from typing import List, Optional
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig

logger = logging.getLogger(__name__)

class LocalFileSource(SourceBase):
    def __init__(self):
        super().__init__()

    def dlt_source_system(self, uri: str, resources: List[ResourceConfig], identity: str):
        """Fetch multiple resources from the local filesystem and create a dlt source."""
        base_path = self._parse_local_uri(uri)
        resources_list = []

        for resource_config in resources:
            table_name = resource_config.source_table_name
            logger.info(f"Processing table: {table_name}")

            reader = self._get_reader(table_name)

            file_incremental = dlt.sources.incremental("modification_date")
            row_incremental = self._get_row_incremental(resource_config)

            file_resource = filesystem(bucket_url=base_path, file_glob=f"{table_name}*")
            file_resource.apply_hints(incremental=file_incremental)

            @dlt.resource(
                name=table_name,
                incremental=row_incremental,
                write_disposition=resource_config.write_disposition,
                primary_key=resource_config.primary_key,
                merge_key=resource_config.merge_key,
                columns=resource_config.columns,
            )
            def data_iterator():
                yield from file_resource | reader()

            resources_list.append(data_iterator)

        return dlt.source(
            schema=dlt.Schema(identity),
            section="local_file_source",
            resources=resources_list
        )

    def _parse_local_uri(self, uri: str) -> str:
        """Extracts the base path from a local filesystem URI or native path."""
        parsed_uri = urllib.parse.urlparse(uri)
        if parsed_uri.scheme == "file":
            return parsed_uri.path
        else:
            raise ValueError(f"Unsupported URI scheme for local filesystem: {uri}")

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
