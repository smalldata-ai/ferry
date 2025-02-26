import dlt
import duckdb
from urllib.parse import urlparse
from ferry.src.sources.source_base import SourceBase
from ferry.src.exceptions import InvalidSourceException
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator  # Import the validator class

class DuckDBSource(SourceBase):
    def __init__(self, uri: str):  # Accept the uri in the constructor
        self.uri = uri
        self.validate_uri(uri)  # Validate the URI when initializing
        super().__init__()

    def validate_uri(self, uri: str):
        """Call the centralized URI validator for DuckDB"""
        DatabaseURIValidator.validate_duckdb(uri)  # Use the validate_duckdb method

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):  # type: ignore
        # Extract DuckDB file path from URI
        database_path = uri.replace("duckdb:///", "")

        # Connect to DuckDB
        conn = duckdb.connect(database_path)

        # Check if the table exists
        existing_tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
        table_names = [t[0] for t in existing_tables]

        if table_name not in table_names:
            conn.close()
            raise ValueError(f"⚠️ Table '{table_name}' not found in DuckDB!")

        # Fetch data
        df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        conn.close()

        # Convert to dlt source format
        def generator():
            for record in df.to_dict(orient="records"):
                yield record

        return dlt.resource(generator(), name=table_name)
