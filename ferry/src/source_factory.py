from urllib.parse import urlparse

from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.source_base import SourceBase
from ferry.src.sources.duckdb_source import DuckDBSource
from ferry.src.sources.postgres_source import PostgresSource

class SourceFactory:
    _items = {
        "postgres": PostgresSource,
        "postgresql": PostgresSource,
        "duckdb": DuckDBSource,
    }

    @staticmethod
    def get(uri: str) -> SourceBase:
        fields = urlparse(uri)
        if fields.scheme in SourceFactory._items:
            class_ = SourceFactory._items.get(fields.scheme, SourceBase)
            return class_() # type: ignore
        else:
            raise InvalidSourceException