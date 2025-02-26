import dlt
from dlt.sources.sql_database import sql_database
from ferry.src.sources.source_base import SourceBase
from urllib.parse import urlparse
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator  # Import the centralized validator

class PostgresSource(SourceBase):
    def __init__(self, uri: str):  # Accept the uri in the constructor
        self.uri = uri
        self.validate_uri(uri)  # Validate the URI when initializing
        super().__init__()

    def validate_uri(self, uri: str):
        """Call the centralized URI validator for PostgreSQL"""
        DatabaseURIValidator.validate_uri(uri)

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):  # type: ignore
        """Create a dlt source from the PostgreSQL URI and table name."""
        
        # Extract credentials from URI and create a DLT source
        credentials = super().create_credentials(uri)
        
        # Create and return the DLT source with the specified table
        source = sql_database(credentials)
        return source.with_resources(table_name)
