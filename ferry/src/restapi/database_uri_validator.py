from urllib.parse import urlparse, parse_qs
from pydantic import field_validator

class DatabaseURIValidator:
    """Validates various database URIs: PostgreSQL, DuckDB, S3."""

    @field_validator("source_uri")
    @classmethod
    def validate_source_uri(cls, v: str) -> str:
        if not v:
            raise ValueError("Source URI must be provided")

        parsed = urlparse(v)
        scheme = parsed.scheme.lower()

        if scheme == "postgresql":
            return cls._validate_postgres_uri(v)
        elif scheme == "duckdb":
            return cls._validate_duckdb_uri(v)
        elif scheme == "s3":
            return cls._validate_s3_uri(v)
        else:
            raise ValueError(f"Unsupported source URI scheme: {scheme}")

    @field_validator("destination_uri")
    @classmethod
    def validate_destination_uri(cls, v: str) -> str:
        if not v:
            raise ValueError("Destination URI must be provided")

        parsed = urlparse(v)
        scheme = parsed.scheme.lower()

        if scheme == "postgresql":
            return cls._validate_postgres_uri(v)
        elif scheme == "duckdb":
            return cls._validate_duckdb_uri(v)
        elif scheme == "s3":
            return cls._validate_s3_uri(v)
        else:
            raise ValueError(f"Unsupported destination URI scheme: {scheme}")

    @classmethod
    def _validate_postgres_uri(cls, v: str) -> str:
        """Validates PostgreSQL URI."""
        parsed = urlparse(v)

        if parsed.scheme != "postgresql":
            raise ValueError("PostgreSQL URI must start with 'postgresql://'")

        netloc = parsed.netloc or ""
        if "@" not in netloc:
            raise ValueError("PostgreSQL URI must contain username and password")

        userinfo, hostport = netloc.split("@")
        if not hostport:
            raise ValueError("PostgreSQL URI must contain host")

        if ":" in hostport:
            host, port = hostport.split(":")
            try:
                if not (0 < int(port) <= 65535):
                    raise ValueError("Invalid PostgreSQL port number")
            except ValueError:
                raise ValueError("PostgreSQL port must be an integer")
        else:
            raise ValueError("PostgreSQL URI must specify a port")

        if not parsed.path or parsed.path == "/":
            raise ValueError("PostgreSQL URI must contain a database name")

        return v

    @classmethod
    def _validate_duckdb_uri(cls, v: str) -> str:
        """Validates DuckDB URI."""
        parsed = urlparse(v)

        if parsed.scheme != "duckdb":
            raise ValueError("DuckDB URI must start with 'duckdb://'")

        if not parsed.path or parsed.path == "/":
            raise ValueError("DuckDB URI must specify a database file path")

        return v

    @classmethod
    def _validate_s3_uri(cls, v: str) -> str:
        """Validates S3 URI."""
        parsed = urlparse(v)

        if parsed.scheme != "s3":
            raise ValueError("S3 URI must start with 's3://'")

        if not parsed.netloc:
            raise ValueError("S3 URI must include a bucket name")

        query_params = parse_qs(parsed.query)

        if "file_key" not in query_params:
            raise ValueError("S3 URI must include a 'file_key' parameter in the query string")

        return v

    @field_validator("source_table_name")
    @classmethod
    def validate_source_table_name(cls, v: str) -> str:
        if not v:
            raise ValueError("Source Table Name must be provided")
        return v

    @field_validator("destination_table_name")
    @classmethod
    def validate_destination_table_name(cls, v: str) -> str:
        if not v:
            raise ValueError("Destination Table Name must be provided")
        return v

    @field_validator("dataset_name")
    @classmethod
    def validate_dataset_name(cls, v: str) -> str:
        if not v:
            raise ValueError("Dataset Name must be provided")
        return v
