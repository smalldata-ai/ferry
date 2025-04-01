import dlt
from ferry.src.destinations.destination_base import DestinationBase


class DuckDBDestination(DestinationBase):
    def default_schema_name(self) -> str:
        return "main"

    def dlt_target_system(self, uri: str, **kwargs):  # type: ignore
        return dlt.destinations.duckdb(uri)
