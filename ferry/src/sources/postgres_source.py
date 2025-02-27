
import dlt
from urllib.parse import urlparse

from src.sources.source_base import SourceBase
from dlt.sources.sql_database import sql_database

class PostgresSource(SourceBase):

  def dlt_source_name(self, uri: str, table_name: str, **kwargs):
    fields = urlparse(uri)
    database_name = fields.path.lstrip('/')
    f"src_{fields.scheme}_{database_name}_{table_name}"

  
  def dlt_source_system(self, uri: str, table_name: str, **kwargs):
    credentials = super().create_credentials(uri)
    source = sql_database(credentials)
    return source.with_resources(table_name)