from urllib.parse import urlparse
from ferry.src.sources.duckdb_source import DuckDBSource
from ferry.src.sources.postgres_source import PostgresSource
from ferry.src.sources.source_base import SourceBase
from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.s3_source import S3Source  # Import S3Source


class SourceFactory:
    _items = {
        "postgres": PostgresSource,
        "postgresql": PostgresSource,
        "duckdb": DuckDBSource,  # Added DuckDB support
        "s3": S3Source, 
    }

    @staticmethod
    def get(uri: str) -> SourceBase:
        """Get the appropriate source object based on the URI"""
        
        # Parse URI
        parsed_uri = urlparse(uri)

        if parsed_uri.scheme in SourceFactory._items:
            # Pass the URI to the source class constructor
            class_ = SourceFactory._items.get(parsed_uri.scheme)
            return class_(uri)  # Pass the URI to the class constructor
        else:
            raise InvalidSourceException(f"Invalid source URI scheme: {parsed_uri.scheme}")
