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
    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        pass


    def create_credentials(self, uri: str):
        return ConnectionStringCredentials(uri)