from abc import ABC, abstractmethod
from typing import Optional, Union
from datetime import datetime, timezone

from ferry.src.restapi.models import (
    DeleteInsertConfig, MergeStrategy, SCD2Config, UpsertConfig
)

import dlt
from dlt.sources.sql_database import sql_database
from dlt.common.typing import TColumnNames
from dlt.common.schema.typing import (
    TScd2StrategyDict, TMergeDispositionDict, TTableSchemaColumns
)
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.extract.resource import DltResource

class SourceBase(ABC):


    @abstractmethod
    def dlt_source_name(self, uri: str, table_name: str, **kwargs):
        pass
    
    @abstractmethod
    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        pass


    # def dlt_merge_resource_system(
    #         self,
    #         uri: str,
    #         table_name: str,
    #         merge_strategy: MergeStrategy = MergeStrategy.DELETE_INSERT,
    #         merge_config: Optional[MergeConfigType] = None,
    #         **kwargs
    # ) -> DltResource:
        
    #     credentials = self.create_credentials(uri)
    #     source = sql_database(credentials)
    #     write_disposition: Union[TMergeDispositionDict, TScd2StrategyDict] = {
    #         "disposition": "merge",
    #         "strategy": "delete-insert"
    #     }
    #     columns: TTableSchemaColumns = {}

    #     primary_key: Optional[TColumnNames] = None
    #     merge_key: Optional[TColumnNames] = None
        

    #     if merge_strategy == MergeStrategy.DELETE_INSERT and isinstance(merge_config, DeleteInsertConfig):
    #         primary_key = merge_config.primary_key
    #         merge_key = merge_config.merge_key

    #         if merge_config.hard_delete_column:
    #             columns[merge_config.hard_delete_column] = {"hard_delete": True}
    #         if merge_config.dedup_sort_column:
    #             columns.update({k: {"dedup_sort": v.value} for k, v in merge_config.dedup_sort_column.items()})

    #     elif merge_strategy == MergeStrategy.SCD2 and isinstance(merge_config, SCD2Config):
            
    #         if merge_config.natural_merge_key:
    #             merge_key = merge_config.natural_merge_key
    #         elif merge_config.partition_merge_key:
    #             merge_key = merge_config.partition_merge_key

    #         scd2_write_disposition: TScd2StrategyDict = {
    #             "disposition": "merge",
    #             "strategy": "scd2"
    #         }

    #         if merge_config.validity_column_names:
    #             scd2_write_disposition["validity_column_names"] = merge_config.validity_column_names
    #         if merge_config.active_record_timestamp:
    #             scd2_write_disposition["active_record_timestamp"] = merge_config.active_record_timestamp
    #         if merge_config.use_boundary_timestamp:
    #             scd2_write_disposition["boundary_timestamp"] = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat()
            
    #         write_disposition = scd2_write_disposition

    #     elif merge_strategy == MergeStrategy.UPSERT and isinstance(merge_config, UpsertConfig):
    #         primary_key = merge_config.primary_key
    #         write_disposition = {
    #             "disposition": "merge",
    #             "strategy": "upsert"
    #         }
    #         if merge_config.hard_delete_column:
    #             columns[merge_config.hard_delete_column] = {"hard_delete": True}

    #     @dlt.resource(
    #         name=table_name,
    #         write_disposition=write_disposition,
    #         primary_key=primary_key if primary_key is not None else [],
    #         merge_key=merge_key if merge_key is not None else [],
    #         columns=columns,
    #     )
    #     def resource_function():
    #         yield from source.with_resources(table_name)

    #     return resource_function


    def create_credentials(self, uri: str):
        return ConnectionStringCredentials(uri)