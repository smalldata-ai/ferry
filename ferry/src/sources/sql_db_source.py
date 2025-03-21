from typing import List
import dlt
from dlt.sources.sql_database import sql_database
from ferry.src.sources.source_base import SourceBase
import logging

logger = logging.getLogger(__name__)

class SqlDbSource(SourceBase):
    
    def __init__(self):
        super().__init__()

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        """Creates a DLT source for a single table."""
        credentials = self.create_credentials(uri)
        sql_source = sql_database(credentials)

        return sql_source.with_resources(table_name)

            resources_list.append(resource_function())

        return DltSource(
            schema=dlt.Schema("sql_db_source"),
            section="sql_db_source",
            resources=resources_list
        )