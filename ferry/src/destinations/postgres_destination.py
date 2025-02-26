import dlt
from ferry.src.destinations.destination_base import DestinationBase
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator  # Import the centralized validator

class PostgresDestination(DestinationBase):

    def validate_uri(self, uri: str):
        """Use centralized URI validation for PostgreSQL."""
        DatabaseURIValidator.validate_uri(uri)

    def dlt_target_system(self, uri: str, **kwargs):  # type: ignore
        # Validate the URI before proceeding
        self.validate_uri(uri)

        # Return the PostgreSQL destination for dlt
        return dlt.destinations.postgres(credentials=uri, **kwargs)
