from urllib.parse import urlparse
from pydantic import BaseModel, Field, field_validator
from typing import Literal, Optional

class LoadDataRequest(BaseModel):
    source_uri: str = Field(..., description="URI of the source database")
    destination_uri: str = Field(..., description="URI of the destination database")
    source_table_name: str = Field(..., description="Name of the source table")
    destination_table_name: str = Field(..., description="Name of the destination table")
    dataset_name: str = Field(..., description="Name of the dataset")

    @field_validator("source_uri")
    @classmethod
    def validate_source_uri(cls, v: str) -> str:
        if not v:
            raise ValueError("Source URI must be provided")
        return cls._validate_uri(v)

    @field_validator("destination_uri")
    @classmethod
    def validate_destination_uri(cls, v: str) -> str:
        if not v:
            raise ValueError("Destination URI must be provided")
        return cls._validate_uri(v)

    @classmethod
    def _validate_uri(cls, v: str) -> str:
        parsed = urlparse(v)

        if parsed.scheme == "duckdb":
            # Extracting the actual database path
            db_path = parsed.netloc + parsed.path
            if not db_path or db_path == "/":
                raise ValueError("Database file path is required for DuckDB")
            return v  # DuckDB URIs are now correctly validated

        elif parsed.scheme in ["postgresql", "clickhouse"]:
            netloc = parsed.netloc or ""
            if not netloc:
                raise ValueError("Username, password, and host are required")

            if "@" not in netloc:
                raise ValueError("Username and password are required")

            userinfo, hostport = netloc.split("@")
            if not hostport:
                raise ValueError("Host is required")

            if ":" in hostport:
                host, port = hostport.split(":")
                try:
                    port_num = int(port)
                    if not (0 < port_num <= 65535):
                        raise ValueError("Invalid port")
                except ValueError:
                    raise ValueError("Invalid port")
            else:
                raise ValueError("Port is required")

            if not hostport:
                raise ValueError("Host is required")

            path = parsed.path or ""
            if not path or path == "/":
                raise ValueError("Database name is required")

            if not userinfo:
                raise ValueError("Username and password are required")
            if ":" not in userinfo:
                raise ValueError("Password is required")
            username, password = userinfo.split(":")
            if not username:
                raise ValueError("Username is required")

        else:
            raise ValueError(f"Unsupported database scheme: {parsed.scheme}")

        return v

class LoadDataResponse(BaseModel):
    status: Literal["success", "error"] = Field(..., description="Status of the data load operation")
    message: str = Field(..., description="Message describing the outcome")
    pipeline_name: Optional[str] = Field(None, description="Name of the pipeline")
    table_processed: Optional[str] = Field(None, description="Name of the table processed")
        

