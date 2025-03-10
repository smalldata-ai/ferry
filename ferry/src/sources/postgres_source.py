import logging
import dlt
from dlt.sources.sql_database import sql_database
from ferry.src.sources.source_base import SourceBase
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator

# Configure logging
logging.basicConfig(level=logging.DEBUG)

class PostgresSource(SourceBase):
    def __init__(self, uri: str):
        self.uri = uri
        self.validate_uri(uri)
        super().__init__()

    def validate_uri(self, uri: str):
        DatabaseURIValidator.validate_uri(uri)

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        """Create a dlt source from the PostgreSQL URI and table name."""
        
        logging.info(f"Connecting to PostgreSQL with URI: {uri}")

        # Extract credentials and create DLT source
        credentials = super().create_credentials(uri)
        source = sql_database(credentials)
        table_resource = source.with_resources(table_name)

        # Fetch sample data for debugging
        data_sample = list(table_resource)
        logging.info(f"Sample data from PostgreSQL ({table_name}): {data_sample[:5]}")

        return table_resource
