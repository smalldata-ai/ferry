from urllib.parse import urlparse

from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.source_base import SourceBase
from ferry.src.sources.duckdb_source import DuckDBSource
from ferry.src.sources.postgres_source import PostgresSource
from ferry.src.sources.s3_source import S3Source  # Import S3Source class

class SourceFactory:
    _items = {
        "postgres": PostgresSource,
        "postgresql": PostgresSource,
        "duckdb": DuckDBSource,  # Ensure DuckDBSource is included
        "s3": S3Source,
    }

    @staticmethod
    def get(uri: str) -> SourceBase:
        fields = urlparse(uri)
        if fields.scheme in SourceFactory._items:
            class_ = SourceFactory._items.get(fields.scheme, SourceBase)

            if class_ == PostgresSource:
                return class_(uri)  # Pass the URI to the PostgresSource constructor
            elif class_ == DuckDBSource:
                return class_(uri)  # Pass the URI to the DuckDBSource constructor
            else:
                return class_(uri)  # For S3 or other classes, pass the URI directly
        else:
            raise InvalidSourceException(f"Invalid source scheme provided: {fields.scheme}")
