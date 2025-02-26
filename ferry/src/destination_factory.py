from urllib.parse import urlparse
from ferry.src.destinations.clickhouse_destination import ClickhouseDestination
from ferry.src.destinations.postgres_destination import PostgresDestination
from ferry.src.destinations.destination_base import DestinationBase
from ferry.src.exceptions import InvalidDestinationException
from ferry.src.destinations.duckdb_destination import DuckDBDestination

class DestinationFactory:
    _items = {
        "postgres": PostgresDestination,
        "postgresql": PostgresDestination,
        "clickhouse": ClickhouseDestination,
        "duckdb": DuckDBDestination,  # Added DuckDB support
    }

    @staticmethod
    def get(uri: str) -> DestinationBase:
        # Validate the URI
        DestinationFactory.validate_uri(uri)
        
        fields = urlparse(uri)
        if fields.scheme in DestinationFactory._items:
            class_ = DestinationFactory._items.get(fields.scheme, DestinationBase)
            return class_()
        else:
            raise InvalidDestinationException(f"Invalid destination URI scheme: {fields.scheme}")

    @staticmethod
    def validate_uri(uri: str):
        """Validate that the destination URI is well-formed."""
        parsed_uri = urlparse(uri)

        # Ensure the scheme is valid (should be one of the known schemes)
        if parsed_uri.scheme not in DestinationFactory._items:
            raise InvalidDestinationException(f"Invalid destination scheme '{parsed_uri.scheme}'. Expected one of: {', '.join(DestinationFactory._items.keys())}.")

        # Additional checks for each scheme
        if parsed_uri.scheme == "postgres" or parsed_uri.scheme == "postgresql":
            # Example: Postgres should have a valid hostname and database
            if not parsed_uri.hostname or not parsed_uri.path:
                raise InvalidDestinationException(f"Invalid Postgres URI: {uri}. Hostname and database are required.")
        
        if parsed_uri.scheme == "clickhouse":
            # Example: ClickHouse should have a valid hostname
            if not parsed_uri.hostname:
                raise InvalidDestinationException(f"Invalid ClickHouse URI: {uri}. Hostname is required.")
        
        if parsed_uri.scheme == "duckdb":
            # Call DuckDB-specific validation
            duckdb_destination = DuckDBDestination()
            duckdb_destination.validate_duckdb_uri(uri)
