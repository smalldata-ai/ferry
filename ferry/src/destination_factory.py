from urllib.parse import urlparse
from src.destinations.clickhouse_destination import ClickhouseDestination
from src.destinations.postgres_destination import PostgresDestination
from src.destinations.destination_base import DestinationBase
from src.exceptions import InvalidDestinationException


class DestinationFactory:
  _items = {
    "postgres": PostgresDestination,
    "clickhouse": ClickhouseDestination,
  }

  @staticmethod
  def get(uri: str) -> DestinationBase:
    fields = urlparse(uri)
    if fields.scheme in DestinationFactory._items:
      class_ =  DestinationFactory._items.get(fields.scheme, DestinationBase)
      return class_()
    else:
      raise InvalidDestinationException()

