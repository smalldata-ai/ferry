from urllib.parse import urlparse, parse_qs
from pydantic import field_validator
from urllib.parse import urlparse

from urllib.parse import urlparse, ParseResult


class DatabaseURIValidator:
    """Validates database URIs: PostgreSQL, DuckDB, S3."""

    @classmethod
    def validate_uri(cls, v: str) -> str:
        """Validates both source and destination URIs."""
        
        if not isinstance(v, str):  # ✅ Ensure `v` is always a string
            raise ValueError(f"URI must be a string, got {type(v).__name__}")

        if not v:
            raise ValueError("URI must be provided")

        parsed = urlparse(v)  # Now we are sure `v` is a string
        scheme = parsed.scheme.lower()

        if scheme in ["postgresql", "postgres","postgresql+psycopg2"]:
            return cls._validate_postgres_uri(v)
        elif scheme == "duckdb":
            return cls._validate_duckdb_uri(v)
        elif scheme == "s3":
            return cls._validate_s3_uri(parsed)
        else:
            raise ValueError(f"Unsupported URI scheme: {scheme}")

    @classmethod
    def _validate_postgres_uri(cls, v: str) -> str:
        """Validates PostgreSQL URI."""
        parsed = urlparse(v)

        if parsed.scheme not in ["postgresql", "postgres"]:
            raise ValueError("PostgreSQL URI must start with 'postgresql://' or 'postgres://'")

        if not parsed.hostname:
            raise ValueError("PostgreSQL URI must include a hostname")

        if not parsed.path or parsed.path == "/":
            raise ValueError("PostgreSQL URI must specify a database name")

        return v  # ✅ Valid Postgres URI

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
    def _validate_s3_uri(cls, v: str):
        """Extracts and validates S3 parameters from the URI."""
        if not isinstance(v, ParseResult):
            parsed = urlparse(v)
        else:
            parsed = v


        print(f"Raw parsed S3 URI: {parsed}")
        
        if not parsed.scheme or parsed.scheme != "s3":
            raise ValueError(f"Invalid URI scheme: {parsed.scheme}")

        query_params = parse_qs(parsed.query)
        print(f"Extracted Query Params: {query_params}")

        # Extract required parameters
        access_key = query_params.get("access_key_id", [None])[0]
        secret_key = query_params.get("access_key_secret", [None])[0]
        region = query_params.get("region", [None])[0]
        bucket_name = parsed.netloc  # Bucket name is in netloc
        file_key = parsed.path.lstrip("/")  # Remove leading "/"

        print(f"Extracted S3 Parameters: access_key={access_key}, secret_key={secret_key}, region={region}, bucket={bucket_name}, file_key={file_key}")

        if not all([access_key, secret_key, region, bucket_name, file_key]):
            raise ValueError("Missing required S3 parameters")

        return {
            "access_key": access_key,
            "secret_key": secret_key,
            "region": region,
            "bucket_name": bucket_name,
            "file_key": file_key,
        }



    @field_validator("source_table_name", "destination_table_name", "dataset_name")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Value must be provided")
        return v
