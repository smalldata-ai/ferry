import dlt
from urllib.parse import urlparse
from ferry.src.destinations.destination_base import DestinationBase
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator  # Import the centralized validator

class DuckDBDestination(DestinationBase):

    def validate_uri(self, uri: str):
        """Use centralized URI validation for DuckDB."""
        DatabaseURIValidator.validate_duckdb(uri)  # Call the centralized validator for DuckDB URI

    def dlt_target_system(self, uri: str, **kwargs):  # type: ignore
        # Validate the URI before proceeding
        self.validate_uri(uri)

        # Extract DuckDB file path from URI
        database_path = uri.replace("duckdb:///", "")

        # Return the DuckDB destination for dlt
        return dlt.destinations.duckdb(configuration={"database": database_path}, **kwargs)
