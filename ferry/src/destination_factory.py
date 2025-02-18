from urllib.parse import urlparse

from ferry.src.destinations.clickhouse_destination import ClickhouseDestination
from ferry.src.destinations.postgres_destination import PostgresDestination
from ferry.src.destinations.destination_base import DestinationBase
from ferry.src.exceptions import InvalidDestinationException


class DestinationFactory:
    _items = {
        "postgres": PostgresDestination,
        "postgresql": PostgresDestination,
        "clickhouse": ClickhouseDestination,
    }

    @staticmethod
    def get(uri: str) -> DestinationBase:
        fields = urlparse(uri)
        if fields.scheme in DestinationFactory._items:
            class_ = DestinationFactory._items.get(fields.scheme, DestinationBase)
            return class_()
        else:
            raise InvalidDestinationException()