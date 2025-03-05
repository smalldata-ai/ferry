
from urllib.parse import urlparse

from ferry.src.sources.sql_source import SqlSource
from urllib.parse import urlparse

class PostgresSource(SqlSource):
    
    def __init__(self, uri: str):
        self.uri = uri
        super().__init__()

    def dlt_source_name(self, uri: str, table_name: str) -> str:
      fields = urlparse(uri)
      database_name = fields.path.lstrip('/')
      return f"src-{fields.scheme}_{database_name}_{table_name}"
    