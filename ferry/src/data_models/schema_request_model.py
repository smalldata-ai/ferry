from pydantic import BaseModel, Field


class SchemaRequest(BaseModel):
    """Request model for schema requests."""
    pipeline_name: str = Field(..., description="Name of the pipeline for which to retrieve the schema")
