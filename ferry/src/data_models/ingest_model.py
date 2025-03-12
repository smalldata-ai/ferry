from typing import Optional, Tuple, Union
from urllib.parse import urlparse
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum

from ferry.src.data_models.incremental_config_model import IncrementalConfig
from ferry.src.data_models.merge_config_model import MergeConfig, MergeStrategy
from ferry.src.data_models.replace_config_model import ReplaceConfig
from ferry.src.uri_validator import URIValidator

class WriteDispositionType(Enum):
    REPLACE = "replace"
    APPEND = "append"
    MERGE = "merge"

class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"    

class DestinationMeta(BaseModel):
    """Configuration for optional destination parameters"""
    table_name: Optional[str] = Field(None, description="Name of the destination table")
    dataset_name: Optional[str] = Field(None, description="Name of the dataset")

class IngestModel(BaseModel):
    """Model for loading data between databases"""
    identity: str = Field(..., description="Identity for the pipeline")
    source_uri: str = Field(..., description="URI of the source database")
    destination_uri: str = Field(..., description="URI of the destination database")
    source_table_name: str = Field(..., description="Name of the source table")
    destination_meta: Optional[DestinationMeta] = Field(None, description="Optional configuration for destination database")
    incremental_config: Optional[IncrementalConfig] = Field(None, description="Incremental config params for loading data")
    write_disposition: Optional[WriteDispositionType] = Field(WriteDispositionType.REPLACE, description="Write Disposition type for loading data")
    replace_config: Optional[ReplaceConfig] = Field(None, description="Configuration for full replace loading")
    merge_config: Optional[MergeConfig] = Field(None, description="Configuration for merge incremental loading")

    @field_validator("source_uri", "destination_uri")
    @classmethod
    def validate__uri(cls, v: str) -> str:
        if not v:
            raise ValueError("URI must be provided")
        return URIValidator.validate_uri(v)
    
    @field_validator("identity","source_table_name")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must be provided")
        return v
    
    @model_validator(mode='after')
    def validate_write_disposition_config(self) -> 'WriteDispositionType':
        if self.write_disposition == WriteDispositionType.APPEND:
            if self.replace_config is not None or self.merge_config is not None:
                raise ValueError("No config is accepted when write_disposition is 'append'")
        elif self.write_disposition == WriteDispositionType.MERGE:
            if self.merge_config is None:
                raise ValueError("merge_config is required when write_disposition is 'merge'")
            if self.replace_config is not None:
                raise ValueError("Only merge_config is accepted when write_disposition is 'merge'")
        elif self.write_disposition == WriteDispositionType.REPLACE:
            if self.merge_config is not None:
                raise ValueError("Only replace_config is accepted when write_disposition is 'replace'")
        else:
            raise ValueError(f"Unsupported write_disposition: {self.write_disposition}")
        return self
    
    def build_wd_config(self):
        if self.write_disposition == WriteDispositionType.APPEND or self.write_disposition == WriteDispositionType.REPLACE:
            return self.write_disposition.value
        elif self.write_disposition == WriteDispositionType.MERGE :
            config = {"disposition": self.write_disposition.value,"strategy": self.merge_config.strategy.value }
            if self.merge_config.strategy == MergeStrategy.SCD2:
                config.update(self.merge_config.scd2_config.build_write_disposition_params())
            return config
        else :
            return WriteDispositionType.REPLACE.value
    