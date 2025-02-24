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
        return cls._validate_uri(v, ["postgresql", "clickhouse", "duckdb"], "Source Database")

    @field_validator("destination_uri")
    @classmethod
    def validate_destination_uri(cls, v: str) -> str:
        if not v:
            raise ValueError("Destination URI must be provided")
        return cls._validate_uri(v, ["postgresql", "clickhouse", "duckdb"], "Destination Database")

    
    
    @field_validator("source_table_name", "destination_table_name", "dataset_name")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    @classmethod
    def _validate_uri(cls, v: str, allowed_schemes: list, db_type: str) -> str:
        """Validates that the URI follows the expected format for PostgreSQL, ClickHouse, or DuckDB."""
        parsed = urlparse(v)
        scheme = parsed.scheme

        if not scheme or scheme not in allowed_schemes:
            raise ValueError(f"{db_type} URL must start with one of {allowed_schemes}")

        if scheme == "duckdb":
            # DuckDB uses a file path; ensure it includes a valid `.duckdb` file
            path = parsed.path.lstrip("/")  # Remove leading `/` to handle relative paths
            if not path or not path.endswith(".duckdb"):
                raise ValueError(f"{db_type} must specify a valid DuckDB file path (e.g., 'duckdb:///path/to/db.duckdb')")
            return v


        # PostgreSQL and ClickHouse validation (expecting user:pass@host:port/dbname)
        netloc = parsed.netloc or ""
        if not netloc:
            raise ValueError(f"{db_type} must include credentials and host")

        if "@" not in netloc:
            raise ValueError(f"{db_type} must include username and password (e.g., user:pass@host)")

        userinfo, hostport = netloc.split("@")
        if not hostport:
            raise ValueError(f"{db_type} must include a valid host")

        if ":" in hostport:
            host, port = hostport.split(":")
            try:
                port_num = int(port)
                if not (0 < port_num <= 65535):
                    raise ValueError("Invalid port number")
            except ValueError:
                raise ValueError("Invalid port number")
        else:
            raise ValueError(f"{db_type} must include a port number")

        if not host:
            raise ValueError(f"{db_type} must include a valid host")

        path = parsed.path or ""
        if not path or path == "/":
            raise ValueError(f"{db_type} must specify a valid database name")

        if ":" not in userinfo:
            raise ValueError(f"{db_type} must include a password (e.g., user:pass@host)")

        return v

class LoadDataResponse(BaseModel):
    status: Literal["success", "error"] = Field(..., description="Status of the data load operation")
    message: str = Field(..., description="Message describing the outcome")
    pipeline_name: Optional[str] = Field(None, description="Name of the pipeline")
    table_processed: Optional[str] = Field(None, description="Name of the table processed")
