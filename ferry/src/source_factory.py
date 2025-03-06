from urllib.parse import urlparse
from ferry.src.sources.source_base import SourceBase
from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.s3_source import S3Source
from ferry.src.sources.sql_db_source import SqlDbSource  # Import S3Source


class SourceFactory:
    _items = {
        "postgres": SqlDbSource,
        "postgresql": SqlDbSource,
        "duckdb": SqlDbSource,  # Added DuckDB support
        "s3": S3Source, 
        "sqlite": SqlDbSource
    }

    @staticmethod
    def get(uri: str) -> SourceBase:
        """Get the appropriate source object based on the URI"""
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme in SourceFactory._items:
            class_ = SourceFactory._items.get(parsed_uri.scheme)
            return class_()  # Pass the URI to the class constructor
        else:
            raise InvalidSourceException(f"Invalid source URI scheme: {parsed_uri.scheme}")
