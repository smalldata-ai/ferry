from urllib.parse import parse_qs, urlparse
import dlt
from src.destinations.destination_base import DestinationBase
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseCredentials,
)

class ClickhouseDestination(DestinationBase):

  def dlt_target_system(self, uri: str, **kwargs):
    parsed_uri = urlparse(uri)
    query_params = parse_qs(parsed_uri.query)
    credentials = ClickHouseCredentials(
      {
          "host": parsed_uri.hostname,
          "port": parsed_uri.port,
          "username": parsed_uri.username,
          "password": parsed_uri.password,
          "database": parsed_uri.path.lstrip("/"),
          "http_port": query_params["http_port"][0],
          "secure": 0,
      }
    )
    return dlt.destinations.clickhouse(credentials=credentials)