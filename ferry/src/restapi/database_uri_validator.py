from urllib.parse import urlparse, parse_qs
from ferry.src.exceptions import InvalidSourceException, InvalidDestinationException

class DatabaseURIValidator:

    @staticmethod
    def validate_common(uri: str):
        """Perform common URI validation for all databases."""
        parsed_uri = urlparse(uri)

        # Ensure URI scheme exists
        if not parsed_uri.scheme:
            raise ValueError(f"Invalid URI: Scheme is missing.")

        # Ensure URI has a valid hostname or path (depending on the scheme)
        if not parsed_uri.hostname and not parsed_uri.path:
            raise ValueError(f"Invalid URI: Missing hostname or path.")

    @staticmethod
    def validate_postgres(uri: str):
        """Validate the PostgreSQL URI."""
        DatabaseURIValidator.validate_common(uri)
        parsed = urlparse(uri)

        # Ensure the scheme is 'postgres' or 'postgresql'
        if parsed.scheme not in ["postgres", "postgresql"]:
            raise InvalidDestinationException(f"Invalid scheme: Expected 'postgres' or 'postgresql', got '{parsed.scheme}'")

        # Check for required hostname and database
        if not parsed.hostname or not parsed.path:
            raise InvalidDestinationException(f"Invalid Postgres URI: Hostname and database are required.")

    @staticmethod
    def validate_clickhouse(uri: str):
        """Validate the ClickHouse URI."""
        DatabaseURIValidator.validate_common(uri)
        parsed = urlparse(uri)

        # Ensure the scheme is 'clickhouse'
        if parsed.scheme != "clickhouse":
            raise InvalidDestinationException(f"Invalid scheme: Expected 'clickhouse', got '{parsed.scheme}'")

        # Check for required hostname
        if not parsed.hostname:
            raise InvalidDestinationException(f"Invalid ClickHouse URI: Hostname is required.")

    @staticmethod
    def validate_duckdb(uri: str):
        """Validate the DuckDB URI."""
        DatabaseURIValidator.validate_common(uri)
        parsed = urlparse(uri)

        # Ensure the scheme is 'duckdb'
        if parsed.scheme != "duckdb":
            raise InvalidDestinationException(f"Invalid scheme: Expected 'duckdb', got '{parsed.scheme}'")

        # Ensure a valid DuckDB file path is provided
        path = parsed.path.lstrip("/")  # Remove leading `/` for relative paths
        if not path or not path.endswith(".duckdb"):
            raise InvalidDestinationException("DuckDB must specify a valid `.duckdb` file path (e.g., 'duckdb:///path/to/db.duckdb')")

    @staticmethod
    def validate_s3(uri: str):
        """Validate S3 URI separately from databases."""
        parsed_uri = urlparse(uri)

        print(f"DEBUG: Parsed URI -> scheme: {parsed_uri.scheme}, netloc: {parsed_uri.netloc}, path: {parsed_uri.path}, query: {parsed_uri.query}")

        # Ensure the scheme is 's3'
        if parsed_uri.scheme != "s3":
            raise ValueError(f"Invalid scheme '{parsed_uri.scheme}'. Expected 's3'.")

        # Ensure that at least a bucket name is present
        if not parsed_uri.netloc:
            raise ValueError("Invalid S3 URI: Missing bucket name.")

        # Path should not be empty (file or folder key inside bucket)
        if "file_key" not in parsed_uri.query:
            raise ValueError("Invalid S3 URI: Missing file key inside the bucket.")