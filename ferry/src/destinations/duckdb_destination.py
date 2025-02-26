import dlt
from urllib.parse import urlparse
from ferry.src.destinations.destination_base import DestinationBase

class DuckDBDestination(DestinationBase):

    def validate_duckdb_uri(self, uri: str):
        """Validates that the DuckDB URI follows the expected format."""
        parsed = urlparse(uri)
        if parsed.scheme != "duckdb":
            raise ValueError(f"Invalid scheme: Expected 'duckdb', got '{parsed.scheme}'")
        
        path = parsed.path.lstrip("/")  # Remove leading `/` for relative paths
        if not path or not path.endswith(".duckdb"):
            raise ValueError("DuckDB must specify a valid `.duckdb` file path (e.g., 'duckdb:///path/to/db.duckdb')")

    def dlt_target_system(self, uri: str, **kwargs):  # type: ignore
        # Validate the URI before proceeding
        self.validate_duckdb_uri(uri)

        # Extract DuckDB file path from URI
        database_path = uri.replace("duckdb:///", "")

        # Return the DuckDB destination for dlt
        return dlt.destinations.duckdb(configuration={"database": database_path}, **kwargs)
