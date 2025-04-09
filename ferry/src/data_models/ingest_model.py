from typing import Any, List, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum
import hashlib
from typing import List, Optional, Dict

from ferry.src.data_models.incremental_config_model import IncrementalConfig
from ferry.src.data_models.merge_config_model import MergeConfig, MergeStrategy
from ferry.src.data_models.replace_config_model import ReplaceConfig, ReplaceStrategy
from ferry.src.uri_validator import URIValidator

class WriteDispositionType(str,Enum):
    REPLACE = "replace"
    APPEND = "append"
    MERGE = "merge"

class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"

class WriteDispositionConfig(BaseModel):
    """Configuration and strategy details for different write dispositions."""
    
    type: WriteDispositionType = Field(WriteDispositionType.REPLACE, description="Type of write disposition")
    
    strategy: Optional[str] = Field(None, description="Strategy for selected write disposition.")
    config: Optional[dict[str, Any]] = Field(None, description="Extra configuration for the selected disposition and strategy")

    @model_validator(mode='after')
    def validate_write_disposition_config(self) -> 'WriteDispositionConfig':
            if self.type == WriteDispositionType.APPEND:
                if self.strategy is not None or self.config is not None:
                    raise ValueError("No strategy or config is accepted when write_disposition type is 'append'")
            
            elif self.type == WriteDispositionType.REPLACE:
                if self.config is not None:
                    raise ValueError("Config is not accepted when write_disposition is 'replace'")
                if self.strategy is None:
                    self.strategy = ReplaceStrategy.TRUNCATE_INSERT.value
                elif self.strategy not in ReplaceStrategy._value2member_map_:
                    raise ValueError("Invalid replace strategy")
            
            elif self.type == WriteDispositionType.MERGE:
                if self.strategy is None:
                    self.strategy = MergeStrategy.DELETE_INSERT.value
                elif self.strategy not in MergeStrategy._value2member_map_:
                    raise ValueError("Invalid merge strategy")

            else:
                raise ValueError(f"Unsupported write disposition type: {self.type}")

            return self


class ResourceConfig(BaseModel):
    """Configuration for a single resource"""
    source_table_name: str = Field(..., description="Name of the source table")
    destination_table_name: Optional[str] = Field(None, description="Name of the destination table")
    column_rules: Optional[Dict[str, List[str]]] = Field(None, description="Column rules for exclusion and pseudonymization")

    incremental_config: Optional[IncrementalConfig] = Field(None, description="Incremental config params for loading data")
    write_disposition_config: Optional[WriteDispositionConfig] = Field(None, description="Write disposition type and configuration for multiple strategies.")

    @field_validator("source_table_name")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must be provided")
        return v
    
    def validate_type(value):
        if value not in WriteDispositionType.__members__.values():
            raise ValueError(f"Unsupported write disposition type: {value}")
        return value

    def build_wd_config(self):
        if self.write_disposition_config is None:
            return WriteDispositionType.REPLACE.value

        if self.write_disposition_config.type == WriteDispositionType.APPEND:
            return self.write_disposition_config.type
        elif self.write_disposition_config.type == WriteDispositionType.REPLACE:
            return {
                "disposition": self.write_disposition_config.type,
                "strategy": self.write_disposition_config.strategy
            }
        elif self.write_disposition_config.type == WriteDispositionType.MERGE:
            config = {
                "disposition": self.write_disposition_config.type,
                "strategy": self.write_disposition_config.strategy
            }
            if self.write_disposition_config.strategy == MergeStrategy.SCD2.value:
                config.update(self.write_disposition_config.config or {})
            return config
        else :
            return WriteDispositionType.REPLACE.value
        
    def get_destination_table_name(self) -> str:
        if self.destination_table_name is None:
            return self.source_table_name
        return self.destination_table_name

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
        return getattr(self.dataset_name, 'dataset_name', self.dataset_name) if self.dataset_name else default_schema_name
