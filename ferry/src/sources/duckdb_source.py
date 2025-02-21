import logging
import duckdb
import dlt

logger = logging.getLogger(__name__)

class DuckDBSource:
    @staticmethod
    def dlt_source_system(source_uri: str, source_table_name: str):
        """Create a DuckDB dlt source."""
        logger.info(f"Initializing DuckDB source for URI: {source_uri}, Table: {source_table_name}")

        try:
            if not source_uri or not source_table_name:
                logger.error("Source URI or table name is missing")
                raise ValueError("Both source URI and table name must be provided")

            logger.info(f"Connecting to DuckDB file: {source_uri}")
            conn = duckdb.connect(source_uri)
            
            # Log connection success
            logger.info(f"DuckDB connection established successfully: {source_uri}")

            # Log available tables in the database
            tables = conn.execute("SHOW TABLES").fetchall()
            logger.info(f"Available tables in {source_uri}: {tables}")

            # Log schema for the requested table
            schema = conn.execute(f"PRAGMA table_info({source_table_name})").fetchall()
            logger.info(f"Schema of table {source_table_name}: {schema}")

            return dlt.source(
                source=lambda: conn.execute(f"SELECT * FROM {source_table_name}").fetchall(),
                name=source_table_name
            )

        except Exception as e:
            logger.error(f"Error initializing DuckDB source: {str(e)}", exc_info=True)
            raise ValueError(f"Error initializing DuckDB source: {str(e)}")
