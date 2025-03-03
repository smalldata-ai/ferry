from pydantic import BaseModel, Field, field_validator

class AppendConfig(BaseModel):
    """Configuration for full loading with different replace strategies"""
    incremental_key: str = Field(..., description="Key used to append data incrementally")

    @field_validator("incremental_key")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must be provided")
        return v
