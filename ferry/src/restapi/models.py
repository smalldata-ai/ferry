from pydantic import BaseModel, Field, field_validator
from typing import Literal, Optional

class LoadDataRequest(BaseModel):
    source_uri: str = Field(..., description="URI of the source database")
    destination_uri: str = Field(..., description="URI of the destination database")
    source_table_name: str = Field(..., description="Name of the source table")
    destination_table_name: str = Field(..., description="Name of the destination table")
    dataset_name: str = Field(..., description="Name of the dataset")

    @field_validator("source_uri", "destination_uri")
    @classmethod
    def validate_uri(cls, v: str) -> str:
        """Ensures the URI is provided but leaves scheme validation to database-specific implementations."""
        if not v:
            raise ValueError("URI must be provided")
        return v

    @field_validator("source_table_name", "destination_table_name", "dataset_name")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        """Ensures table names and dataset names are not empty."""
        if not v:
            raise ValueError("Field must not be empty")
        return v

class LoadDataResponse(BaseModel):
    status: Literal["success", "error"] = Field(..., description="Status of the data load operation")
    message: str = Field(..., description="Message describing the outcome")
    pipeline_name: Optional[str] = Field(None, description="Name of the pipeline")
    table_processed: Optional[str] = Field(None, description="Name of the table processed")
  