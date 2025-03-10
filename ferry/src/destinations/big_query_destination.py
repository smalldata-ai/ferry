from urllib.parse import parse_qs, urlparse

import dlt
from dlt.common.configuration.specs.gcp_credentials import GcpOAuthCredentialsWithoutDefaults
from ferry.src.destinations.destination_base import DestinationBase


class BigqueryDestination(DestinationBase):

    def default_schema_name(self):
        return ""
    
    def dlt_target_system(self, uri: str, **kwargs): # type: ignore
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)

        return dlt.destinations.bigquery(credentials= GcpOAuthCredentialsWithoutDefaults())
        # return dlt.destinations.bigquery(
        #     credentials=credentials,  # type: ignore
        #     location=location,
        #     project_id=project_id,
        #     **kwargs,
        # )
