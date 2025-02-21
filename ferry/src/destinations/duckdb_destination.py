import dlt

class DuckDBDestination:
    @staticmethod
    def dlt_target_system(destination_uri: str):
        """Ensures DuckDB is recognized as a valid destination"""
        if destination_uri.startswith("duckdb://") or destination_uri.endswith(".duckdb"):
            return "duckdb"  # DLT expects a string identifier, not a connection object
        raise ValueError("Invalid DuckDB URI format")

class DestinationFactory:
    @staticmethod
    def get(destination_uri: str):
        """Returns the appropriate destination handler based on URI"""
        # if destination_uri.startswith("postgresql://"):
        #     return PostgresDestination()
        # elif destination_uri.startswith("clickhouse://"):
        #     return ClickhouseDestination()
        if destination_uri.startswith("duckdb://") or destination_uri.endswith(".duckdb"):
            return DuckDBDestination()
        else:
            raise ValueError("Unsupported destination type")
