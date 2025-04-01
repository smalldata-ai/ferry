import dlt
from ferry.src.destinations.destination_base import DestinationBase


class SnowflakeDestination(DestinationBase):
    def default_schema_name(self) -> str:
        return "public"

    def dlt_target_system(self, uri: str, **kwargs):
        return dlt.destinations.snowflake(credentials=uri, **kwargs)
