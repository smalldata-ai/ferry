from urllib.parse import urlparse

from ferry.src.destinations.clickhouse_destination import ClickhouseDestination
from ferry.src.destinations.postgres_destination import PostgresDestination
from ferry.src.destinations.duckdb_destination import DuckDBDestination
from ferry.src.destinations.s3_destination import S3Destination  # Import S3 destination
from ferry.src.destinations.destination_base import DestinationBase
from ferry.src.exceptions import InvalidDestinationException


class DestinationFactory:
    _items = {
        "postgres": PostgresDestination,
        "postgresql": PostgresDestination,
        "clickhouse": ClickhouseDestination,
        "duckdb": DuckDBDestination,  
        "s3": S3Destination  # Add S3 as a valid destination
    }

    @staticmethod
    def get(uri: str) -> DestinationBase:
        fields = urlparse(uri)
        if fields.scheme in DestinationFactory._items:
            class_ = DestinationFactory._items.get(fields.scheme, DestinationBase)
            return class_()
        else:
            raise InvalidDestinationException(f"Invalid destination type: {fields.scheme}")
