import dlt

from ferry.src.destinations.destination_base import DestinationBase

class PostgresDestination(DestinationBase):

    def dlt_target_system(self, uri: str, **kwargs): # type: ignore
        return dlt.destinations.postgres(credentials=uri, **kwargs)