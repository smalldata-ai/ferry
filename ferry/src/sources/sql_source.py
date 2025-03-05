import dlt

from src.sources.source_base import SourceBase
from dlt.sources.sql_database import sql_database
from ferry.src.sources.source_base import SourceBase

class SqlSource(SourceBase):
    
    def __init__(self):
        super().__init__()
    
    def dlt_source_name(self, uri: str, table_name: str) -> str:
        pass
    
    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        credentials = super().create_credentials(uri)
        source = sql_database(credentials)
        incremental = None
        if kwargs.get("incremental_config"):
            incremental_config = kwargs.get("incremental_config")
            incremental = dlt.sources.incremental(
                cursor_path=incremental_config.get("incremental_key", None),
                initial_value=incremental_config.get("start_position", None),
                range_start=incremental_config.get("range_start", None),
                end_value=incremental_config.get("end_position", None),
                range_end=incremental_config.get("range_end", None),
                lag=incremental_config.get("lag_window", 0),
                )

        @dlt.resource(
              name=table_name,
              incremental=incremental,
              write_disposition=kwargs.get("write_disposition", ""),
              primary_key=kwargs.get("primary_key", ""),
              merge_key=kwargs.get("merge_key", ""),
              columns=kwargs.get("columns", None),
          )
        def resource_function():
            yield from source.with_resources(table_name)

        return resource_function          
    
    