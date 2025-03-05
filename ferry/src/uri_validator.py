from urllib.parse import urlparse, parse_qs

class URIValidator:
    """Validates URIs: PostgreSQL, DuckDB, S3."""

    @classmethod
    def validate_uri(cls, v: str) -> str:
        """Validates both source and destination URIs."""
        if not v:
            raise ValueError("URI must be provided")

        parsed = urlparse(v)
        scheme = parsed.scheme.lower()

        if scheme == "postgresql":
            return cls._validate_postgres_uri(v)
        elif scheme == "duckdb":
            return cls._validate_duckdb_uri(v)
        elif scheme == "s3":
            return cls._validate_s3_uri(v)
        elif scheme == "clickhouse":
            return cls._validate_clickhouse_uri(v)
        else:
            raise ValueError(f"Unsupported URI scheme: {scheme}")

    @classmethod
    def _validate_postgres_uri(cls, v: str) -> str:
        """Validates PostgreSQL URI."""
        parsed = urlparse(v)

        if parsed.scheme != "postgresql":
            raise ValueError("PostgreSQL URI must start with 'postgresql://'")

        if "@" not in (parsed.netloc or ""):
            raise ValueError("PostgreSQL URI must contain username and password")

        userinfo, hostport = parsed.netloc.split("@", 1)
        if ":" not in hostport:
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
    
    @classmethod
    def _validate_clickhouse_uri(cls, v: str) -> str:
        """Validates Clickhouse URI."""
        parsed = urlparse(v)

        if parsed.scheme != "clickhouse":
            raise ValueError("Clickhouse URI must start with 'clickhouse://'")

        if "@" not in (parsed.netloc or ""):
            raise ValueError("Clickhouse URI must contain username and password")
        
        userinfo, hostport = parsed.netloc.split("@", 1)
        username, _, password = userinfo.partition(":")
        
        if not username:
            raise ValueError("Clickhouse URI must contain a non-empty username")
        
        if ":" not in hostport:
            raise ValueError("Clickhouse URI must specify a host and port")
        
        host, port = hostport.split(":", 1)
        
        if not host:
            raise ValueError("Clickhouse URI must specify a non-empty host")

        if not port.isdigit():
            raise ValueError("Clickhouse URI port must be an integer")

        if not parsed.path or parsed.path == "/":
            raise ValueError("Clickhouse URI must contain a database name")

        return v