from typing import Optional, Union
from datetime import datetime, timezone

from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource

from ferry.src.restapi.models import (
    DeleteInsertConfig, MergeConfigTypeContainer, MergeStrategy, SCD2Config, UpsertConfig
)
from ferry.src.sources.source_base import SourceBase

import dlt
from dlt.sources.sql_database import sql_database
from dlt.common.typing import TColumnNames
from dlt.common.schema.typing import (
    TScd2StrategyDict, TMergeDispositionDict, TTableSchemaColumns
)


class PostgresSource(SourceBase):

    MergeConfigType = MergeConfigTypeContainer.MergeConfigType

    def dlt_source_system( # type: ignore
            self,
            uri: str,
            table_name: str,
            **kwargs
    ) -> DltSource:
        
        credentials = super().create_credentials(uri)
        source = sql_database(credentials)
        return source.with_resources(table_name)


    def dlt_merge_resource_system( # type: ignore
            self,
            uri: str,
            table_name: str,
            merge_strategy: MergeStrategy = MergeStrategy.DELETE_INSERT,
            merge_config: Optional[MergeConfigType] = None,
            **kwargs
    ) -> DltResource:
        return super().dlt_merge_resource_system(uri, table_name, merge_strategy, merge_config, **kwargs)