import dlt
from ferry.src.destinations.destination_base import DestinationBase


class PostgresDestination(DestinationBase):
    def default_schema_name(self) -> str:
        return "public"

    def dlt_target_system(self, uri: str, **kwargs):
        return dlt.destinations.postgres(credentials=uri, **kwargs)
