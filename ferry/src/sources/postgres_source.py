import dlt
from dlt.sources.sql_database import sql_database
from ferry.src.sources.source_base import SourceBase
from urllib.parse import urlparse

class PostgresSource(SourceBase):
    def __init__(self, uri: str):  # Accept the uri in the constructor
        self.uri = uri
        # Parse the URI to extract credentials, host, etc., if necessary
        self.credentials = self.create_credentials(uri)
        super().__init__()

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):  # type: ignore
        """Validates PostgreSQL URI and creates a dlt source."""

        # Parse the URI
        parsed = urlparse(uri)
        scheme = parsed.scheme

        if scheme != "postgresql":
            raise ValueError("Invalid scheme for PostgreSQL. Expected 'postgresql://'")

        # Validate host, credentials, and database name
        if not parsed.netloc or "@" not in parsed.netloc:
            raise ValueError("PostgreSQL URI must include credentials (e.g., user:pass@host:port/dbname)")

        userinfo, hostport = parsed.netloc.split("@", 1)
        if ":" not in userinfo:
            raise ValueError("PostgreSQL URI must include username and password (e.g., user:pass@host)")

        if "/" not in parsed.path or not parsed.path.strip("/"):
            raise ValueError("PostgreSQL URI must specify a valid database name")

        # Extract credentials and create a DLT source
        credentials = super().create_credentials(uri)
        source = sql_database(credentials)
        return source.with_resources(table_name)
