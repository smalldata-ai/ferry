from abc import ABC, abstractmethod
from typing import Any, List, Dict
import hashlib
import logging
import dlt
from dlt.extract.source import DltSource
from dlt.sources.credentials import ConnectionStringCredentials
from ferry.src.data_models.ingest_model import ResourceConfig, WriteDispositionType
from ferry.src.data_models.merge_config_model import MergeConfig, MergeStrategy

logger = logging.getLogger(__name__)

class SourceBase(ABC):

    @abstractmethod
    def dlt_source_system(self, uri: str, resources: List[ResourceConfig], identity: str) -> DltSource:
        pass

    def create_credentials(self, uri: str):
        return ConnectionStringCredentials(uri)

    def _pseudonymize_columns(self, row: Dict, pseudonymizing_columns: List[str]) -> Dict:
        """Pseudonymizes specified columns using SHA-256 hashing."""
        salt = "WI@N57%zZrmk#88c"
        for col in pseudonymizing_columns:
            if col in row and row[col] is not None:
                sh = hashlib.sha256()
                sh.update((str(row[col]) + salt).encode())
                row[col] = sh.hexdigest()
        return row

    def _create_dlt_resource(self, resource_config: ResourceConfig, data_iterator):
        """Creates a DLT resource dynamically."""
        exclude_columns = resource_config.column_rules.get("exclude_columns", []) if resource_config.column_rules else []
        pseudonymizing_columns = resource_config.column_rules.get("pseudonymizing_columns", []) if resource_config.column_rules else []

        incremental_column = None
        primary_key, merge_key, columns = [], [], []
        if resource_config.incremental_config:
            incremental_config = resource_config.incremental_config.build_config()
            incremental_column = incremental_config.get("incremental_key", None)

        write_disposition = resource_config.build_wd_config()
        if resource_config.write_disposition_config and resource_config.write_disposition_config.type == WriteDispositionType.MERGE.value:
            strategy = resource_config.write_disposition_config.strategy
            config = resource_config.write_disposition_config.config or {}
            merge_config = MergeConfig(strategy=MergeStrategy(strategy), **config)
            primary_key = merge_config.build_pk_config()
            merge_key = merge_config.build_merge_key()
            columns = merge_config.build_columns()
        

        @dlt.resource(
            name=resource_config.get_destination_table_name(),
            incremental=dlt.sources.incremental(incremental_column) if incremental_column else None,
            write_disposition=write_disposition,
            primary_key=primary_key,
            merge_key=merge_key,
            columns=columns,
        )
        def resource_function():

            if not exclude_columns and not pseudonymizing_columns:
                                yield from data_iterator
                                return

            for row in data_iterator:
                if not isinstance(row, dict):
                    logger.warning(f"Skipping non-dictionary row: {row}")
                    continue

                if exclude_columns:
                    row = {k: v for k, v in row.items() if k not in exclude_columns}
                if pseudonymizing_columns:
                    row = self._pseudonymize_columns(row, pseudonymizing_columns)

                logger.debug(f"Processed row: {row}")
                yield row

        return resource_function()
