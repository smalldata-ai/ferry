from urllib.parse import urlparse
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, List, Optional, Tuple, Union
from enum import Enum

from ferry.src.exceptions import InvalidDestinationException, InvalidSourceException
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator

class WriteDispositionType(Enum):
    REPLACE = "replace"
    APPEND = "append"
    MERGE = "merge"

class MergeStrategy(Enum):
    DELETE_INSERT = "delete-insert"
    SCD2 = "scd2"
    UPSERT = "upsert"

class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"    

class DeleteInsertConfig(BaseModel):
    """Configuration for delete-insert merge strategy"""
    primary_key: Optional[Union[str, Tuple[str, ...]]] = Field(None, description="Primary key(s) for delete-merge strategy")
    merge_key: Optional[Union[str, Tuple[str, ...]]] = Field(None, description="Merge key(s) for delete-merge strategy")
    hard_delete_column: Optional[str] = Field(None, description="Column used to mark records for deletion from the destination dataset")
    dedup_sort_column: Optional[Dict[str, SortOrder]] = Field(None, description="Column used to sort records before deduplication, following the specified order")

    @model_validator(mode='after')
    def validate_keys(self) -> 'DeleteInsertConfig':
        if not self.primary_key and not self.merge_key:
            raise ValueError("At least one of 'primary' or 'merge' key(s) must be provided for delete-insert strategy")
        return self    

class MergeConfig(BaseModel):
    """Configuration for incremental loading with different merge strategies"""
    merge_strategy: MergeStrategy = Field(MergeStrategy.DELETE_INSERT, description="Strategy for merging data")
    delete_insert_config: Optional[DeleteInsertConfig] = Field(None, description="Configuration for delete-insert merge strategy")
    # scd2_config: Optional[SCD2Config] = Field(None, description="Configuration for SCD2 merge strategy")
    # upsert_config: Optional[UpsertConfig] = Field(None, description="Configuration for upsert merge strategy")

    @model_validator(mode='after')
    def validate_strategy_config(self) -> 'MergeConfig':
        if self.merge_strategy == MergeStrategy.DELETE_INSERT:
            if self.delete_insert_config is None:
                raise ValueError("delete_insert_config is required when merge_strategy is 'delete-insert'")
            if self.scd2_config is not None or self.upsert_config is not None:
                raise ValueError("Only delete_insert_config is accepted when merge_strategy is 'delete-insert'")
        # elif self.merge_strategy == MergeStrategy.SCD2:
        #     if self.scd2_config is None:
        #         raise ValueError("scd2_config is required when merge_strategy is 'scd2'")
        #     if self.delete_insert_config is not None or self.upsert_config is not None:
        #         raise ValueError("Only scd2_config is accepted when merge_strategy is 'scd2'")
        # elif self.merge_strategy == MergeStrategy.UPSERT:
        #     if self.upsert_config is None:
        #         raise ValueError("upsert_config is required when merge_strategy is 'upsert'")
        #     if self.delete_insert_config is not None or self.scd2_config is not None:
        #         raise ValueError("Only upsert_config is accepted when merge_strategy is 'upsert'")
        else:
            raise ValueError(f"Unsupported merge strategy: {self.merge_strategy}")
        return self


class WriteDisposition(Enum):
    """Configuration for optional write disposition parameters"""
    type: Optional[WriteDispositionType] = Field(WriteDispositionType.REPLACE, description="Write Disposition type for loading data")
    merge_config: Optional[MergeConfig] = Field(None, description="Configuration for merge incremental loading")


class DestinationMeta(BaseModel):
    """Configuration for optional destination parameters"""
    table_name: Optional[str] = Field(..., description="Name of the destination table")
    dataset_name: Optional[str] = Field(..., description="Name of the dataset")

class IngestModel(BaseModel):
    """Model for loading data between databases"""
    source_uri: str = Field(..., description="URI of the source database")
    destination_uri: str = Field(..., description="URI of the destination database")
    source_table_name: str = Field(..., description="Name of the source table")
    destination_meta: Optional[DestinationMeta] = Field(None, description="Optional configuration for destination database")
    merge_config: Optional[MergeConfig] = Field(None, description="Configuration for merge incremental loading")

    @field_validator("source_uri", "destination_uri")
    @classmethod
    def validate__uri(cls, v: str) -> str:
        if not v:
            raise ValueError("URI must be provided")
        return DatabaseURIValidator.validate_uri(v)
    
    @field_validator("source_table_name")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must be provided")
        return v


