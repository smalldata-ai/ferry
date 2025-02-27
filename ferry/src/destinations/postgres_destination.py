import dlt
from urllib.parse import urlparse
from ferry.src.destinations.destination_base import DestinationBase

class PostgresDestination(DestinationBase):
    
    def dlt_destination_name(self, uri: str, table_name: str, **kwargs):
      fields = urlparse(uri)
      database_name = fields.path.lstrip('/')
      f"dest_{fields.scheme}_{database_name}_{table_name}"

    def dlt_target_system(self, uri: str, **kwargs): # type: ignore
        return dlt.destinations.postgres(credentials=uri, **kwargs)