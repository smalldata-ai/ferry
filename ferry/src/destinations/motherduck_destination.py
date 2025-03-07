from urllib.parse import parse_qs, urlparse

import dlt

from ferry.src.destinations.destination_base import DestinationBase


class MotherduckDestination(DestinationBase):

    def default_schema_name(self):
        return "main"
    
    def dlt_target_system(self, uri: str, **kwargs): # type: ignore
        return dlt.destinations.motherduck(credentials=uri, **kwargs)

