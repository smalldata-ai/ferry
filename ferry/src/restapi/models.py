from urllib.parse import urlparse
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, List, Optional, Tuple, Union
from enum import Enum

from ferry.src.exceptions import InvalidDestinationException, InvalidSourceException


class WriteDisposition(Enum):
    REPLACE = "replace"
    APPEND = "append"
    MERGE = "merge"

class MergeStrategy(Enum):
    DELETE_INSERT = "delete-insert"
    SCD2 = "scd2"
    UPSERT = "upsert"

class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"

class LoadStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"

class DeleteInsertConfig(BaseModel):
    """Configuration for delete-insert merge strategy"""
    primary_key: Optional[Union[str, Tuple[str, ...]]] = Field(None, description="Primary key(s) for delete-merge strategy")
    merge_key: Optional[Union[str, Tuple[str, ...]]] = Field(None, description="Merge key(s) for delete-merge strategy")
    hard_delete_column: Optional[str] = Field(None, description="Column used to mark records for deletion from the destination dataset")
    dedup_sort_column: Optional[Dict[str, SortOrder]] = Field(None, description="Column used to sort records before deduplication, following the specified order")

    @model_validator(mode='after')
    def validate_keys(self) -> 'DeleteInsertConfig':
        if not self.primary_key and not self.merge_key:
            raise ValueError("At least one of 'primary' or 'merge' key(s) must be provided for delete-insert strategy")
        return self

class SCD2Config(BaseModel):
    """Configuration for slowly changing dimension type 2 merge strategy"""
    natural_merge_key: Optional[Union[str, Tuple[str, ...]]] = Field(None, description="Key(s) that define unique records. Prevents absent rows from being retired in incremental loads")
    partition_merge_key: Optional[Union[str, Tuple[str, ...]]] = Field(None, description="Key(s) defining partitions. Retires only absent records within loaded partitions")
    validity_column_names: List[str] = Field(["valid_from", "valid_to"], description="Column names for validity periods")
    active_record_timestamp: str = Field("9999-12-31", description="Timestamp value for active records")
    use_boundary_timestamp: bool = Field(False, description="Record validity windows with boundary timestamp")

    @field_validator("validity_column_names")
    @classmethod
    def validate_validity_column_names(cls, v: List[str]) -> List[str]:
        if len(v) != 2:
            raise ValueError("Validity column names must contain exactly two strings")
        return v
    
    @field_validator('natural_merge_key', 'partition_merge_key')
    @classmethod
    def validate_keys(cls, value, values):
        if values.data.get('natural_merge_key') and values.data.get('partition_merge_key'):
            raise ValueError("Only one of 'natural' or 'partition' merge key(s) must be provided for scd2 strategy")
        return value

class UpsertConfig(BaseModel):
    """Configuration for upsert merge strategy"""
    primary_key: Union[str, Tuple[str, ...]] = Field(..., description="Primary key(s) for upsert merge strategy")
    hard_delete_column: Optional[str] = Field(None, description="Column used to mark records for deletion from the destination dataset")

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key(cls, v: Union[str, Tuple[str, ...]]) -> Union[str, Tuple[str, ...]]:
        if isinstance(v, str):
            if not v.strip():
                raise ValueError("Primary key must be provided")
        elif isinstance(v, tuple):
            if not v:
                raise ValueError("At least one primary key must be provided in the tuple")
            if not all(isinstance(key, str) and key.strip() for key in v):
                raise ValueError("All primary keys in the tuple must be non-empty strings")
        return v

class MergeConfigTypeContainer:
    """Container for merge configuration types."""
    MergeConfigType = Union[DeleteInsertConfig, SCD2Config, UpsertConfig]

class MergeIncrementalLoadConfig(BaseModel):
    """Configuration for incremental loading with different merge strategies"""
    merge_strategy: MergeStrategy = Field(MergeStrategy.DELETE_INSERT, description="Strategy for merging data")
    delete_insert_config: Optional[DeleteInsertConfig] = Field(None, description="Configuration for delete-insert merge strategy")
    scd2_config: Optional[SCD2Config] = Field(None, description="Configuration for SCD2 merge strategy")
    upsert_config: Optional[UpsertConfig] = Field(None, description="Configuration for upsert merge strategy")

    @model_validator(mode='after')
    def validate_strategy_config(self) -> 'MergeIncrementalLoadConfig':
        if self.merge_strategy == MergeStrategy.DELETE_INSERT:
            if self.delete_insert_config is None:
                raise ValueError("delete_insert_config is required when merge_strategy is 'delete-insert'")
            if self.scd2_config is not None or self.upsert_config is not None:
                raise ValueError("Only delete_insert_config is accepted when merge_strategy is 'delete-insert'")
        elif self.merge_strategy == MergeStrategy.SCD2:
            if self.scd2_config is None:
                raise ValueError("scd2_config is required when merge_strategy is 'scd2'")
            if self.delete_insert_config is not None or self.upsert_config is not None:
                raise ValueError("Only scd2_config is accepted when merge_strategy is 'scd2'")
        elif self.merge_strategy == MergeStrategy.UPSERT:
            if self.upsert_config is None:
                raise ValueError("upsert_config is required when merge_strategy is 'upsert'")
            if self.delete_insert_config is not None or self.scd2_config is not None:
                raise ValueError("Only upsert_config is accepted when merge_strategy is 'upsert'")
        else:
            raise ValueError(f"Unsupported merge strategy: {self.merge_strategy}")
        return self


class LoadDataRequest(BaseModel):
    """Request model for loading data between databases"""
    source_uri: str = Field(..., description="URI of the source database")
    destination_uri: str = Field(..., description="URI of the destination database")
    source_table_name: str = Field(..., description="Name of the source table")
    destination_table_name: str = Field(..., description="Name of the destination table")
    dataset_name: str = Field(..., description="Name of the dataset")
    write_disposition: WriteDisposition = Field(WriteDisposition.REPLACE, description="Write Disposition type for loading data")
    merge_incremental_load_config: Optional[MergeIncrementalLoadConfig] = Field(None, description="Configuration for merge incremental loading")

    # @field_validator("source_uri")
    # @classmethod
    # def validate_source_uri(cls, v: str) -> str:
    #     if not v:
    #         raise ValueError("Source URI must be provided")
    #     return cls._validate_uri(v, ["postgresql", "clickhouse", "duckdb"], "Source Database")

    # @field_validator("destination_uri")
    # @classmethod
    # def validate_destination_uri(cls, v: str) -> str:
    #     if not v:
    #         raise ValueError("Destination URI must be provided")
    #     return cls._validate_uri(v, ["postgresql", "clickhouse", "duckdb"], "Destination Database")

    
    
    # @field_validator("source_table_name", "destination_table_name", "dataset_name")
    # @classmethod
    # def validate_non_empty(cls, v: str) -> str:
    #     if not v:
    #         raise ValueError("Field must not be empty")
    #     return v

    @classmethod
    def _validate_uri(cls, v: str, uri_type: str) -> str:
        from ferry.src.source_factory import SourceFactory
        from ferry.src.destination_factory import DestinationFactory
        try:
            scheme = urlparse(v).scheme
            if uri_type == "source" and scheme not in SourceFactory._items:
                    raise InvalidSourceException(f"Unsupported {uri_type.capitalize()} URI scheme: {scheme if scheme else 'None'}")
            elif uri_type == "destination" and scheme not in DestinationFactory._items:
                    raise InvalidDestinationException(f"Unsupported {uri_type.capitalize()} URI scheme: {scheme if scheme else 'None'}")
            
            parsed = urlparse(v)

            netloc = parsed.netloc or ""
            if not netloc:
                raise ValueError(f"Username, password, and host are required in {uri_type} URI")
            
            if "@" not in netloc:
                raise ValueError(f"Username and password are required in {uri_type} URI")
            
            userinfo, hostport = netloc.split("@")
            if not hostport:
                raise ValueError(f"Host is required in {uri_type} URI")
            
            if ":" in hostport:
                host, port = hostport.split(":")
                try:
                    port_num = int(port)
                    if not (0 < port_num <= 65535):
                        raise ValueError(f"Invalid port in {uri_type} URI")
                except:
                    raise ValueError(f"Invalid port in {uri_type} URI")
            else:
                raise ValueError(f"Port is required in {uri_type} URI")
            
            if not host:
                raise ValueError(f"Host is required in {uri_type} URI")
            
            path = parsed.path or ""
            if not path or path == "/":
                raise ValueError(f"Database name is required in {uri_type} URI")
            
            if not userinfo:
                raise ValueError(f"Username is required in {uri_type} URI")
            
            return v
        
        except (InvalidSourceException, InvalidDestinationException, ValueError) as e:
            raise e

        except Exception as e:
            raise ValueError(f"Invalid {uri_type} URI: {e}") from e


    @model_validator(mode='after')
    def validate_merge_incremental_config_dependency(self) -> 'LoadDataRequest':
        if self.write_disposition == WriteDisposition.MERGE and self.merge_incremental_load_config is None:
            raise ValueError("merge_incremental_config is required when write_disposition is 'merge'")
        if self.write_disposition != WriteDisposition.MERGE and self.merge_incremental_load_config is not None:
            raise ValueError("write_disposition should be 'merge' if merge_incremental_config is passed")
        if self.write_disposition == WriteDisposition.REPLACE and self.merge_incremental_load_config is not None:
            raise ValueError("merge_incremental_config is not accepted when write_disposition is 'replace'")
        return self


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
    """Response model for data loading operations."""
    status: LoadStatus = Field(..., description="Status of the data load operation")
    message: str = Field(..., description="Message describing the outcome")
    pipeline_name: Optional[str] = Field(None, description="Name of the pipeline")
    table_processed: Optional[str] = Field(None, description="Name of the table processed")
