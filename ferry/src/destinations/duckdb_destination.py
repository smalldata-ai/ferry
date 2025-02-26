import dlt
from ferry.src.destinations.destination_base import DestinationBase

class DuckDBDestination(DestinationBase):

    def dlt_target_system(self, uri: str, **kwargs):  # type: ignore
        # Extract DuckDB file path from URI
        database_path = uri.replace("duckdb:///", "")

        # Return the DuckDB destination for dlt
        return dlt.destinations.duckdb(configuration={"database": database_path}, **kwargs)