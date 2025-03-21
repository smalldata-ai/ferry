import dlt
import logging
import hashlib
from typing import List
from dlt.extract.source import DltSource
from dlt.sources.sql_database import sql_database
from ferry.src.data_models.ingest_model import ResourceConfig
from ferry.src.sources.source_base import SourceBase

logger = logging.getLogger(__name__)

class SqlDbSource(SourceBase):
    
    def __init__(self):
        super().__init__()
    
    def dlt_source_system(self, uri: str, resources: List[ResourceConfig], identity: str) -> DltSource:
        """Creates a DLT source with resources for multiple tables."""
        credentials = self.create_credentials(uri)
        sql_source = sql_database(credentials)
        resources_list = []

        for resource_config in resources:
            table_name = resource_config.source_table_name
            exclude_columns = resource_config.exclude_columns or []
            pseudonymizing_columns = resource_config.pseudonymizing_columns or []

            logger.info(f"Processing table: {table_name}, Excluding columns: {exclude_columns}, Pseudonymizing columns: {pseudonymizing_columns}")
            
            incremental = None
            if resource_config.incremental_config:
                incremental_config = resource_config.incremental_config.build_config()
                incremental = dlt.sources.incremental(
                    cursor_path=incremental_config.get("incremental_key", None),
                    initial_value=incremental_config.get("start_position", None),
                    range_start=incremental_config.get("range_start", None),
                    end_value=incremental_config.get("end_position", None),
                    range_end=incremental_config.get("range_end", None),
                    lag=incremental_config.get("lag_window", 0),
                )
            
            write_disposition = resource_config.build_wd_config()
            primary_key = resource_config.merge_config.build_pk_config() if resource_config.merge_config else []
            merge_key = resource_config.merge_config.build_merge_key() if resource_config.merge_config else []
            columns = resource_config.merge_config.build_columns() if resource_config.merge_config else {}

            def pseudonymize_columns(row):
                """Pseudonymizes specified columns using SHA-256 hashing."""
                salt = "WI@N57%zZrmk#88c"
                for col in pseudonymizing_columns:
                    if col in row and row[col] is not None:
                        sh = hashlib.sha256()
                        sh.update((str(row[col]) + salt).encode())
                        row[col] = sh.hexdigest()
                return row

            @dlt.resource(
                name=resource_config.get_destination_table_name(),
                incremental=incremental,
                write_disposition=write_disposition,
                primary_key=primary_key,
                merge_key=merge_key,
                columns=columns,
            )
            def resource_function():
                data = sql_source.with_resources(table_name)
                for row in data:
                    if not isinstance(row, dict):
                        logger.warning(f"Skipping non-dictionary row: {row}")
                        continue
                    filtered_row = {k: v for k, v in row.items() if k not in exclude_columns}
                    pseudonymized_row = pseudonymize_columns(filtered_row)
                    logger.debug(f"Processed row: {pseudonymized_row}")
                    yield pseudonymized_row

            resources_list.append(resource_function())

        return DltSource(
            schema=dlt.Schema(identity),
            section="sql_db_source",
            resources=resources_list
        )