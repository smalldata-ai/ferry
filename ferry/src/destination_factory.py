from urllib.parse import urlparse
from ferry.src.destinations.athena_destination import AthenaDestination
from ferry.src.destinations.big_query_destination import BigQueryDestination
from ferry.src.destinations.clickhouse_destination import ClickhouseDestination
from ferry.src.destinations.motherduck_destination import MotherduckDestination
from ferry.src.destinations.mssql_destination import MssqlDestination
from ferry.src.destinations.postgres_destination import PostgresDestination
from ferry.src.destinations.destination_base import DestinationBase
from ferry.src.destinations.snowflake_destination import SnowflakeDestination
from ferry.src.destinations.sql_alchemy_destination import SqlAlchemyDestination
from ferry.src.destinations.syanpse_destination import SynapseDestination
from ferry.src.exceptions import InvalidDestinationException
from ferry.src.destinations.duckdb_destination import DuckDBDestination


class DestinationFactory:
    _items = {
        "postgres": PostgresDestination,
        "postgresql": PostgresDestination,
        "clickhouse": ClickhouseDestination,
        "duckdb": DuckDBDestination,
        "snowflake": SnowflakeDestination,
        "md": MotherduckDestination,
        "synapse": SynapseDestination,
        "sqlite": SqlAlchemyDestination,
        "mysql": SqlAlchemyDestination,
        "mssql": MssqlDestination,
        "athena": AthenaDestination,
        "bigquery": BigQueryDestination,
    }

    @staticmethod
    def get(uri: str) -> DestinationBase:
        parsed_uri = urlparse(uri)
        print(f"Parsed URI: {parsed_uri}")  # Debugging output
        print(f"Scheme: {parsed_uri.scheme}, Path: {parsed_uri.path}")

        if parsed_uri.scheme in DestinationFactory._items:
            class_ = DestinationFactory._items.get(parsed_uri.scheme)
            return class_()
        else:
            raise InvalidDestinationException(
                f"Invalid destination URI scheme: {parsed_uri.scheme}"
            )
