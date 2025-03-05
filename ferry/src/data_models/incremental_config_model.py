from datetime import datetime

from enum import Enum
from typing import Dict, Optional, Union
from pydantic import BaseModel, Field, field_validator

class BoundaryMode(Enum):
    START_END = "start-end"
    START = "start"
    END = "end"
    BETWEEN = "between"

class IncrementalConfig(BaseModel):
    incremental_key: str = Field(..., description="Key used to load new data from source")
    start_position: Optional[Union[int, datetime]] = Field(None, description="Start value for incremental loading can be an integer or date time")
    end_position: Optional[Union[int, datetime]] = Field(None, description="End value for incremental loading can be an integer or date time")
    lag_window: Optional[float] = Field(None, description="Float value  used to fetch past data relative to the last incremental data fetched")
    boundary_mode: Optional[BoundaryMode] = Field(None, description="Mode use to determine whether to include start or end cursor positions")
    
    @field_validator("incremental_key")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must be provided")
        return v
    
    def build_config(self) -> Dict:
        return {
            "incremental_key": self.incremental_key,
            "start_position": self.start_position,
            "end_position": self.end_position,
            "lag_window": self.lag_window,
            "range_start": "closed" if self.boundary_mode == BoundaryMode.START or self.boundary_mode == BoundaryMode.START_END  else "open",
            "range_end": "closed" if self.boundary_mode == BoundaryMode.END or self.boundary_mode == BoundaryMode.START_END  else "open",
            "lag_window": self.lag_window,
        }