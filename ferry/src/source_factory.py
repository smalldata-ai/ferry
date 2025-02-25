from urllib.parse import urlparse

from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.postgres_source import PostgresSource
from ferry.src.sources.source_base import SourceBase
from ferry.src.sources.s3_source import S3Source

class SourceFactory:
    _items = {
        "postgres": PostgresSource,
        "postgresql": PostgresSource,
        "s3":S3Source,
    }

    @staticmethod
    def get(uri: str) -> SourceBase:
        fields = urlparse(uri)
        if fields.scheme in SourceFactory._items:
            class_ = SourceFactory._items.get(fields.scheme, SourceBase)
            return class_() # type: ignore
        else:
            raise InvalidSourceException
