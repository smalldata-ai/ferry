import dlt
from urllib.parse import urlparse
from ferry.src.destinations.destination_base import DestinationBase
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator
from ferry.src.exceptions import InvalidSourceException  # Import exception

class PostgresDestination(DestinationBase):
    
    def dlt_destination_name(self, uri: str, table_name: str) -> str:
      fields = urlparse(uri)
      database_name = fields.path.lstrip('/')
      return f"dest_{fields.scheme}_{database_name}_{table_name}"

    def validate_uri(self, uri: str):
        """Use centralized URI validation for PostgreSQL."""
        try:
            DatabaseURIValidator.validate_uri(uri)
        except ValueError as e:
            raise InvalidSourceException(f"Invalid PostgreSQL URI: {e}")

    def dlt_target_system(self, uri: str, **kwargs):  # type: ignore
        """Validate URI and return DLT PostgreSQL destination."""
        self.validate_uri(uri)
        return dlt.destinations.postgres(credentials=uri, **kwargs)
