import dlt
from urllib.parse import parse_qs, urlparse
from ferry.src.destinations.destination_base import DestinationBase

class MotherduckDestination(DestinationBase):

    def default_schema_name(self) -> str:
        return "main"
    
    def dlt_target_system(self, uri: str, **kwargs):
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        token = query_params.get("token", [None])[0]
        md_uri = f'md:{parsed_uri.hostname}?motherduck_token={token}'
        return dlt.destinations.motherduck(credentials=md_uri, **kwargs)

