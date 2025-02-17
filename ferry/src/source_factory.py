from urllib.parse import urlparse
from src.exceptions import InvalidSourceException
from src.sources.postgres_source import PostgresSource
from src.sources.source_base import SourceBase


class SourceFactory:
  _items = {
    "postgres": PostgresSource,
    "postgresql": PostgresSource
  }

  
  @staticmethod
  def get(uri: str) -> SourceBase:
    fields = urlparse(uri)
    if fields.scheme in SourceFactory._items:
      class_ =  SourceFactory._items.get(fields.scheme, SourceBase)
      return class_()
    else:
      raise InvalidSourceException()

