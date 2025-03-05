import dlt
import os
from urllib.parse import urlparse
from ferry.src.destinations.destination_base import DestinationBase

class DuckDBDestination(DestinationBase):

    def dlt_destination_name(self, uri: str, table_name: str, **kwargs):
        fields = urlparse(uri)
        database_name = os.path.basename(fields.path)
        f"dest_{fields.scheme}_{database_name}_{table_name}"
        

    def dlt_target_system(self, uri: str, **kwargs):  # type: ignore

        # Extract DuckDB file path from URI
        database_path = uri.replace("duckdb:///", "")

        # Return the DuckDB destination for dlt
        return dlt.destinations.duckdb(configuration={"database": database_path}, **kwargs)
