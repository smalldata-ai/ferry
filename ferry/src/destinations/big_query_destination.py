from urllib.parse import parse_qs, urlparse

import dlt
from dlt.common.configuration.specs.gcp_credentials import GcpOAuthCredentialsWithoutDefaults
from ferry.src.destinations.destination_base import DestinationBase


class BigQueryDestination(DestinationBase):
    def default_schema_name(self) -> str:
        return "default"

    def dlt_target_system(self, uri: str, **kwargs):
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)

        credentials = GcpOAuthCredentialsWithoutDefaults(
            client_id=query_params.get("client_id", [None])[0],
            client_secret=query_params.get("client_secret", [None])[0],
            refresh_token=query_params.get("refresh_token", [None])[0],
            project_id=parsed_uri.hostname,
        )
        return dlt.destinations.bigquery(credentials=credentials)
