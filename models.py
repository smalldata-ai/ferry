from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Literal

class LoadDataRequest(BaseModel):
    table_names: List[str] = Field(..., description="List of tables to load")
    write_disposition: Literal["merge", "replace"] = Field(
        "merge",
        description="Write disposition ('merge' or 'replace')"
    )
    full_refresh: bool = Field(False, description="Perform a full refresh")

    @field_validator('table_names')
    def validate_table_names(cls, v):
        if not v:
            raise ValueError("At least one table name must be provided")
        return v

class LoadDataResponse(BaseModel):
    status: str
    message: str
    pipeline_name: str
    tables_processed: List[str]