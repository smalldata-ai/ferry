from enum import Enum
from typing import Dict, List, Optional, Tuple, Union
from pydantic import BaseModel, Field, field_validator, model_validator

class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"    


class MergeStrategy(Enum):
    UPDATE_INSERT = "update-insert"
    DELETE_INSERT = "delete-insert"
    SCD2 = "scd2"
    UPSERT = "upsert"

class UpdateInsertConfig(BaseModel):
    """Configuration for update-insert merge strategy"""
    incremental_key: str = Field(..., description="Key used to append data incrementally")
    primary_key: Union[str, Tuple[str, ...]] = Field(..., description="Primary key for update-merge strategy")

    @field_validator("incremental_key", "primary_key")
    @classmethod
    def validate__uri(cls, v: str) -> str:
        if not v:
            raise ValueError("Key must be provided")

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
    
class UpsertConfig(BaseModel):
    """Configuration for upsert merge strategy"""
    primary_key: Union[str, Tuple[str, ...]] = Field(..., description="Primary key(s) for upsert merge strategy")
    hard_delete_column: Optional[str] = Field(None, description="Column used to mark records for deletion from the destination dataset")

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key(cls, v: Union[str, Tuple[str, ...]]) -> Union[str, Tuple[str, ...]]:
        if isinstance(v, str):
            if not v.strip():
                raise ValueError("Primary key must be provided")
        elif isinstance(v, tuple):
            if not v:
                raise ValueError("At least one primary key must be provided in the tuple")
            if not all(isinstance(key, str) and key.strip() for key in v):
                raise ValueError("All primary keys in the tuple must be non-empty strings")
        return v

class SCD2Config(BaseModel):
    """Configuration for slowly changing dimension type 2 merge strategy"""
    natural_merge_key: Optional[Union[str, Tuple[str, ...]]] = Field(None, description="Key(s) that define unique records. Prevents absent rows from being retired in incremental loads")
    partition_merge_key: Optional[Union[str, Tuple[str, ...]]] = Field(None, description="Key(s) defining partitions. Retires only absent records within loaded partitions")
    validity_column_names: List[str] = Field(["valid_from", "valid_to"], description="Column names for validity periods")
    active_record_timestamp: str = Field("9999-12-31", description="Timestamp value for active records")
    use_boundary_timestamp: bool = Field(False, description="Record validity windows with boundary timestamp")

    @field_validator("validity_column_names")
    @classmethod
    def validate_validity_column_names(cls, v: List[str]) -> List[str]:
        if len(v) != 2:
            raise ValueError("Validity column names must contain exactly two strings")
        return v
    
    @field_validator('natural_merge_key', 'partition_merge_key')
    @classmethod
    def validate_keys(cls, value, values):
        if values.data.get('natural_merge_key') and values.data.get('partition_merge_key'):
            raise ValueError("Only one of 'natural' or 'partition' merge key(s) must be provided for scd2 strategy")
        return value    

class MergeConfig(BaseModel):
    """Configuration for incremental loading with different merge strategies"""
    strategy: MergeStrategy = Field(MergeStrategy.DELETE_INSERT, description="Strategy for merging data")
    delete_insert_config: Optional[DeleteInsertConfig] = Field(None, description="Configuration for delete-insert merge strategy")
    scd2_config: Optional[SCD2Config] = Field(None, description="Configuration for SCD2 merge strategy")
    upsert_config: Optional[UpsertConfig] = Field(None, description="Configuration for upsert merge strategy")
    
    @model_validator(mode='after')
    def validate_strategy_config(self) -> 'MergeConfig':
        if self.strategy == MergeStrategy.DELETE_INSERT:
            if self.delete_insert_config is None:
                raise ValueError("delete_insert_config is required when merge_strategy is 'delete-insert'")
            if self.scd2_config is not None or self.upsert_config is not None:
                raise ValueError("Only delete_insert_config is accepted when merge_strategy is 'delete-insert'")
        elif self.strategy == MergeStrategy.SCD2:
            if self.scd2_config is None:
                raise ValueError("scd2_config is required when merge_strategy is 'scd2'")
            if self.delete_insert_config is not None or self.upsert_config is not None:
                raise ValueError("Only scd2_config is accepted when merge_strategy is 'scd2'")
        elif self.strategy == MergeStrategy.UPSERT:
            if self.upsert_config is None:
                raise ValueError("upsert_config is required when merge_strategy is 'upsert'")
            if self.delete_insert_config is not None or self.scd2_config is not None:
                raise ValueError("Only upsert_config is accepted when merge_strategy is 'upsert'")
        else:
            raise ValueError(f"Unsupported merge strategy: {self.merge_strategy}")
        return self
