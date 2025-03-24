import dlt
from ferry.src.destinations.destination_base import DestinationBase

class SynapseDestination(DestinationBase):

    def default_schema_name(self) -> str:
        return "dbo"
    
    def dlt_target_system(self, uri: str, **kwargs): # type: ignore
        return dlt.destinations.synapse(credentials=uri, **kwargs)

