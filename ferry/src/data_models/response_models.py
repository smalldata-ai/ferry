from enum import Enum
from typing import Any, Dict, Optional
from dlt.common.schema.typing import TStoredSchema
from pydantic import BaseModel, Field


class LoadStatus(Enum):
    SUCCESS = "success"
    PROCESSING = "processing"
    ERROR = "error"

class IngestResponse(BaseModel):
    """Response model for data loading operations."""
    status: LoadStatus = Field(..., description="Status of the data load operation")
    message: str = Field(..., description="Message describing the outcome")
    pipeline_name: Optional[str] = Field(None, description="Name of the pipeline")
    schema_version_hash: Optional[str] = Field(None, description="Version hash of the schema")

class SchemaResponse(BaseModel):
    """Response model for schema requests."""
    pipeline_name: Optional[str] = Field(None, description="Name of the pipeline")
    pipeline_schema: Optional[str] = Field(None, description="Schema data")