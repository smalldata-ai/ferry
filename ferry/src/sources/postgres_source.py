import logging
import dlt
from dlt.sources.sql_database import sql_database
from ferry.src.sources.source_base import SourceBase
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator

class PostgresSource(SourceBase):
    def __init__(self, uri: str):
        self.uri = uri
        self.validate_uri(uri)
        super().__init__()

    def validate_uri(self, uri: str):
        DatabaseURIValidator.validate_uri(uri)

    def dlt_source_system(self, uri: str, table_name: str, batch_size=1000, **kwargs):
        """Fetch data in batches and update progress."""
        logging.info(f"Connecting to PostgreSQL with URI: {uri}")
        
        credentials = super().create_credentials(uri)
        source = sql_database(credentials)
        table_resource = source.with_resources(table_name)

        # Convert table resource into an iterator
        data_iterator = iter(table_resource)
        
        def batch_generator():
            batch_number = 0
            while True:
                batch = []
                try:
                    for _ in range(batch_size):  # Fetch batch_size rows
                        batch.append(next(data_iterator))
                except StopIteration:
                    if not batch:
                        break  # Exit if no data left
                
                batch_number += 1
                total_batches = batch_number  # Store total batch count
                
                # Log batch completion
                logging.info(f"Processing batch {batch_number}/{total_batches}")

                yield from batch  # Yield rows one by one

            logging.info(f"✅ All batches completed. Total: {total_batches}")

        return dlt.resource(batch_generator(), name=table_name)
