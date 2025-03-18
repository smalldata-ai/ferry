import dlt

from ferry.src.destinations.destination_base import DestinationBase

class RedshiftDestination(DestinationBase):

    def default_schema_name(self):
        return "public"
    
    def dlt_target_system(self, uri: str, **kwargs): # type: ignore
        return dlt.destinations.redshift(credentials=uri, **kwargs)

