from pydantic import BaseModel, Field
from typing import  Optional
from enum import Enum

class ReplaceStrategy(Enum):
    TRUNCATE_INSERT = "truncate-and-insert"
    INSERT_FROM_STAGING = "insert-from-staging"
    STAGING_OPTIMIZED = "staging-optimized"    


class ReplaceConfig(BaseModel):
    """Configuration for full loading with different replace strategies"""
    strategy: Optional[ReplaceStrategy] = Field(ReplaceStrategy.TRUNCATE_INSERT, description="Strategy for replacing data")
