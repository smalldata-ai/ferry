import dlt
import duckdb
import os
from urllib.parse import urlparse
from ferry.src.sources.source_base import SourceBase

class DuckDBSource(SourceBase):

    def dlt_source_name(self, uri: str, table_name: str, **kwargs):
        fields = urlparse(uri)
        database_name = os.path.basename(fields.path)
        f"src_{fields.scheme}_{database_name}_{table_name}"

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
