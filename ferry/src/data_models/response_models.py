from enum import Enum
from typing import Optional

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
  