import dlt

from src.sources.source_base import SourceBase
from dlt.sources.sql_database import sql_database

class PostgresSource(SourceBase):
  
  def dlt_source_system(self, uri: str, table_name: str, **kwargs):
    credentials = super().create_credentials(uri)
    source = sql_database(credentials)  # type: ignore
    return source.with_resources(table_name)