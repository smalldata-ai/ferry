from pydantic import BaseModel, Field, field_validator
from typing import Literal, Optional
from ferry.src.validation.uri_validator import validate_uri  # Import the function

class LoadDataRequest(BaseModel):
    source_uri: str = Field(..., description="URI of the source database")
    destination_uri: str = Field(..., description="URI of the destination database")
    source_table_name: str = Field(..., description="Name of the source table")
    destination_table_name: str = Field(..., description="Name of the destination table")
    dataset_name: str = Field(..., description="Name of the dataset")

    @field_validator("source_uri")
    @classmethod
    def validate_source_uri(cls, v: str) -> str:
        return validate_uri(v, ["postgresql", "clickhouse", "duckdb"], "Source Database")

    @field_validator("destination_uri")
    @classmethod
    def validate_destination_uri(cls, v: str) -> str:
        return validate_uri(v, ["postgresql", "clickhouse", "duckdb"], "Destination Database")

    @field_validator("source_table_name", "destination_table_name", "dataset_name")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

class LoadDataResponse(BaseModel):
    status: Literal["success", "error"] = Field(..., description="Status of the data load operation")
    message: str = Field(..., description="Message describing the outcome")
    pipeline_name: Optional[str] = Field(None, description="Name of the pipeline")
    table_processed: Optional[str] = Field(None, description="Name of the table processed")
