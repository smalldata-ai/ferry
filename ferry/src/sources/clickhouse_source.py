import dlt
from typing import List
from dlt.sources.sql_database import sql_database
from dlt.extract.source import DltSource
from ferry.src.data_models.ingest_model import ResourceConfig
from dlt.extract.source import DltSource
from dlt.sources.sql_database import sql_database
from ferry.src.data_models.ingest_model import ResourceConfig
from ferry.src.sources.source_base import SourceBase
from ferry.src.sources.sql_db_source import SqlDbSource

class ClickhouseSource(SqlDbSource):
    
    def __init__(self):
        super().__init__()
    
    def dlt_source_system(self, uri: str, resources: List[ResourceConfig], identity: str) -> DltSource:
        """Creates a DLT source for clickhouse with resources for multiple tables."""
        uri = uri.replace("clickhouse", "clickhouse+native")
        return super().dlt_source_system(uri, resources, identity)
