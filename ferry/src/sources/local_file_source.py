import dlt
import urllib.parse
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from ferry.src.sources.source_base import SourceBase

class LocalFileSource(SourceBase):
    def __init__(self):
        super().__init__()

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        """Fetch data from the local filesystem and create a dlt resource."""
        base_path = self._parse_local_uri(uri)
        
        file_resource = self._create_file_resource(base_path, table_name)
        return self._apply_reader(file_resource, table_name)
    
    def _parse_local_uri(self, uri: str) -> str:
        """Extracts the base path from a local filesystem URI or native path."""
        parsed_uri = urllib.parse.urlparse(uri)
        if parsed_uri.scheme == "file":
            return parsed_uri.path
        else:
            raise ValueError(f"Unsupported URI scheme for local filesystem: {uri}")

    def _create_file_resource(self, base_path: str, table_name: str):
        """Creates a dlt file resource with incremental loading."""
        file_resource = filesystem(bucket_url=base_path, file_glob=f"{table_name}*")
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