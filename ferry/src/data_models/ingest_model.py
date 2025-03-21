from typing import List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum
import hashlib

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

class ResourceConfig(BaseModel):
    """Configuration for a single resource"""
    source_table_name: str = Field(..., description="Name of the source table")
    destination_table_name: Optional[str] = Field(None, description="Name of the destination table")
    exclude_columns: Optional[List[str]] = Field(None, description="List of columns to be excluded from ingestion")
    pseudonymizing_columns: Optional[List[str]] = Field(None, description="List of columns to be pseudonymized")

    incremental_config: Optional[IncrementalConfig] = Field(None, description="Incremental config params for loading data")
    write_disposition: Optional[WriteDispositionType] = Field(WriteDispositionType.REPLACE, description="Write disposition type for loading data")
    replace_config: Optional[ReplaceConfig] = Field(None, description="Configuration for full replace loading")
    merge_config: Optional[MergeConfig] = Field(None, description="Configuration for merge incremental loading")

    @field_validator("source_table_name")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must be provided")
        return v

    @model_validator(mode='after')
    def validate_write_disposition_config(self) -> 'ResourceConfig':
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
        return self
        


    def build_wd_config(self):
        if self.write_disposition in (WriteDispositionType.APPEND, WriteDispositionType.REPLACE):
            return self.write_disposition.value
        elif self.write_disposition == WriteDispositionType.MERGE:
            config = {"disposition": self.write_disposition.value, "strategy": self.merge_config.strategy.value}
            if self.merge_config.strategy == MergeStrategy.SCD2:
                config.update(self.merge_config.scd2_config.build_write_disposition_params())
            return config
        return WriteDispositionType.REPLACE.value
    

    def get_destination_table_name(self) -> str:
        return getattr(self.destination_table_name, 'table_name', self.destination_table_name) if self.destination_table_name else self.source_table_name

    
# def pseudonymize_data(self, data: dict) -> dict:
#         """Apply pseudonymization to specified columns"""
#         if self.pseudonymizing_columns:
#             salt = 'WI@N57%zZrmk#88c'
#             for col in self.pseudonymizing_columns:
#                 if col in data:
#                     sh = hashlib.sha256()
#                     sh.update((data[col] + salt).encode())
#                     data[col] = sh.hexdigest()
#         return data

class IngestModel(BaseModel):
    """Model for loading data between databases with multiple resources"""
    identity: str = Field(..., description="Identity for the pipeline")
    source_uri: str = Field(..., description="URI of the source database")
    destination_uri: str = Field(..., description="URI of the destination database")
    dataset_name: Optional[str] = Field(None, description="Name of the dataset")
    resources: List[ResourceConfig] = Field(..., description="List of resources to ingest")

    @field_validator("source_uri", "destination_uri")
    @classmethod
    def validate__uri(cls, v: str) -> str:
        if not v:
            raise ValueError("URI must be provided")
        return URIValidator.validate_uri(v)

    @field_validator("identity")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must be provided")
        return v

    @field_validator("resources")
    @classmethod
    def validate_resources(cls, v: List[ResourceConfig]) -> List[ResourceConfig]:
        if not v:
            raise ValueError("At least one resource must be provided")
        return v
    
    def get_dataset_name(self, default_schema_name: str) -> str:
        return getattr(self.dataset_name, 'dataset_name', default_schema_name) if self.dataset_name else default_schema_name
    
    def get_exclude_columns(self) -> List[str]:
        """Returns a combined list of columns to exclude from all resources."""
        return list({col for resource in self.resources if resource.exclude_columns for col in resource.exclude_columns})

    
    
