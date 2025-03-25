import dlt
import logging
from typing import List
from dlt.extract.source import DltSource
from dlt.sources.sql_database import sql_database
from ferry.src.data_models.ingest_model import ResourceConfig
from ferry.src.sources.source_base import SourceBase

logger = logging.getLogger(__name__)

class SqlDbSource(SourceBase):
    
    def __init__(self):
        super().__init__()
    
    def dlt_source_system(self, uri: str, resources: List[ResourceConfig], identity: str) -> DltSource:
        """Creates a DLT source with resources for multiple tables."""
        credentials = self.create_credentials(uri)
        sql_source = sql_database(credentials)
        resources_list = []

        for resource_config in resources:
            table_name = resource_config.source_table_name  
            logger.info(f"Processing table: {table_name}")

            data_iterator = sql_source.with_resources(table_name)

            resources_list.append(self._create_dlt_resource(resource_config, data_iterator))

        return DltSource(
            schema=dlt.Schema(identity),
            section="sql_db_source",
            resources=resources_list
        )
