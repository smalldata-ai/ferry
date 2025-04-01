import dlt
import urllib.parse
from dlt.common.configuration.specs import AzureCredentials
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from ferry.src.sources.source_base import SourceBase


class AzureStorageSource(SourceBase):
    def __init__(self):
        super().__init__()

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        """Fetch data from Azure Blob Storage and create a dlt resource."""
        container_name, azure_credentials = self._parse_azure_uri(uri)

        file_resource = self._create_file_resource(container_name, azure_credentials, table_name)
        return self._apply_reader(file_resource, table_name)

    def _parse_azure_uri(self, uri: str):
        parsed_uri = urllib.parse.urlparse(uri)
        container_name = parsed_uri.path.lstrip("/").split("/")[0]
        query_params = urllib.parse.parse_qs(parsed_uri.query)

        azure_credentials = AzureCredentials(
            azure_storage_account_name=query_params.get("account_name", [None])[0],
            azure_storage_account_key=query_params.get("account_key", [None])[0],
        )
        return container_name, azure_credentials

    def _create_file_resource(
        self, container_name: str, azure_credentials: AzureCredentials, table_name: str
    ):
        """Creates a dlt file resource with incremental loading."""
        file_resource = filesystem(f"az://{container_name}", azure_credentials, f"{table_name}*")
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
