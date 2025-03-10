import logging
import dlt
from ferry.src.destinations.destination_base import DestinationBase
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator
import logging
logger = logging.getLogger(__name__)
# Configure logging
logging.basicConfig(level=logging.DEBUG)

class DuckDBDestination(DestinationBase):
    def validate_uri(self, uri: str):
        DatabaseURIValidator.validate_uri(uri)

    def dlt_target_system(self, uri: str, **kwargs):
        self.validate_uri(uri)

        # Extract the DuckDB file path correctly
        database_path = uri.replace("duckdb:///", "").strip()
        logger.info(f"✅ Final DuckDB database path: {database_path}")

        # Initialize DuckDB with explicit path
        destination = dlt.destinations.duckdb(database=database_path, **kwargs)

        if destination:
            logger.info("✅ DuckDB destination initialized successfully.")

        return destination
