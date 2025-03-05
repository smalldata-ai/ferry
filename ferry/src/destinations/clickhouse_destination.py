from urllib.parse import parse_qs, urlparse

import dlt

from ferry.src.destinations.destination_base import DestinationBase
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseCredentials,
)

class ClickhouseDestination(DestinationBase):
    
    def dlt_destination_name(self, uri: str, table_name: str) -> str:
        fields = urlparse(uri)
        database_name = fields.path.lstrip('/')
        return f"dest-{fields.scheme}_{database_name}_{table_name}"

    def dlt_target_system(self, uri: str, **kwargs): # type: ignore
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        credentials = ClickHouseCredentials(
            {
                "host": parsed_uri.hostname,
                "port": parsed_uri.port,
                "username": parsed_uri.username,
                "password": parsed_uri.password,
                "database": parsed_uri.path.lstrip("/"),
                "http_port": int(query_params.get("http_port", ["8123"])[0]),
                "secure": int(query_params.get("secure", ["0"])[0]),
            }
        )

        return dlt.destinations.clickhouse(credentials=credentials)