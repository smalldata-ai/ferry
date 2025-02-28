from urllib.parse import urlparse
from ferry.src.destinations.destination_base import DestinationBase
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator
from ferry.src.exceptions import InvalidSourceException  # Import exception
import dlt
class PostgresDestination(DestinationBase):

    def validate_uri(self, uri: str):
        """Use centralized URI validation for PostgreSQL."""
        try:
            DatabaseURIValidator.validate_uri(uri)  # ✅ Pass the string, not parsed object
        except ValueError as e:
            raise InvalidSourceException(f"Invalid PostgreSQL URI: {e}")

    def dlt_target_system(self, uri: str, **kwargs):  # type: ignore
        """Validate URI and return DLT PostgreSQL destination."""
        self.validate_uri(uri)
        return dlt.destinations.postgres(credentials=uri, **kwargs)
