import dlt
from dlt.common.configuration.specs import AwsCredentials
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from ferry.src.sources.source_base import SourceBase

class LocalFileSource(SourceBase):
    def __init__(self):
        super().__init__()

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        """Fetch data from Local filesystem and create a dlt resource."""
        
        file_resource = self._create_file_resource(uri, table_name)
        return self._apply_reader(file_resource, table_name)
    
    
    def _create_file_resource(self, path: str, file_glob: str):
        """Creates a dlt file resource with incremental loading."""
        file_resource = filesystem(path, f"{file_glob}*")
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