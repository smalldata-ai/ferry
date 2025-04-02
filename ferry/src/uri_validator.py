import re
from urllib.parse import urlparse


class URIValidator:
    """Validates URIs for different database systems using regex."""

    SCHEMES = {
        "sql": {
            "postgresql",
            "postgres",
            "clickhouse",
            "redshift",
            "mysql",
            "hana",
            "mssql",
            "mariadb+pymysql",
            "mariadb",
        },
        "filebased": {"duckdb", "sqlite"},
        "mongodb": {"mongodb"},
        "snowflake": {"snowflake"},
        "motherduck": {"md"},
        "bigquery": {"bigquery"},
        "athena": {"athena"},
        "s3": {"s3"},
        "gcs": {"gs"},
        "azure": {"az"},
        "local": {"file"},
    }

    # Regex patterns for URI validation
    URI_PATTERNS = {
        "sql": r"^(?P<scheme>\w+)://(?P<user>\w+)(:(?P<password>[^@]*))?@(?P<host>[\w.-]+)(:(?P<port>\d+))/(?P<db>\w+)",
        "mongodb": r"^mongodb://(?P<user>\w+)(:(?P<password>[^@]+))?@(?P<host>[\w.-]+)(:(?P<port>\d+))/(?P<auth_db>\w+)?\?database=(?P<database>\w+)",
        "snowflake": r"^snowflake://(?P<user>\w+)(:(?P<password>[^@]+))@(?P<account>[\w.-]+)(/(?P<db>\w+))(/(?P<dataset>\w+))",
        "motherduck": r"^md://(?P<database>\w+)\?token=(?P<token>\w+)",
        "bigquery": r"^bigquery://(?P<projectId>\w+)\?client_id=(?P<clientId>[a-zA-Z0-9.-]+)&client_secret=(?P<clientSecret>[a-zA-Z0-9-]+)&refresh_token=(?P<refreshToken>.+)",
        "filebased": r"^(?P<scheme>\w+)://(/)?(?P<path>[a-zA-Z]:[\\/].+|/.+)",
        "s3": r"^s3://(?P<bucket>[\w.-]+)\?access_key_id=(?P<access_key_id>[a-zA-Z0-9]+)&access_key_secret=(?P<access_key_secret>[a-zA-Z0-9]+)&region=(?P<region>[\w-]+)",
        "gcs": r"^gs://(?P<bucket>[\w.-]+)(/(?P<key>.+))?",
        "azure": r"^az://(?P<container>[\w.-]+)(/(?P<blob>.+))?",
        "file": r"^file://(?P<path>/[\w./-]+)",
    }

    @classmethod
    def validate_uri(cls, uri: str) -> str:
        """Validates URIs based on their scheme using regex."""
        if not uri:
            raise ValueError("URI must be provided")

        parsed = urlparse(uri)
        scheme = parsed.scheme.lower()

        for category, schemes in cls.SCHEMES.items():
            if scheme in schemes:
                return cls._apply_regex_validation(category, scheme, uri)

        raise ValueError(f"Unsupported URI scheme: {scheme}")

    @classmethod
    def _apply_regex_validation(cls, category: str, scheme: str, uri: str):
        """Validates the URI using regex matching."""
        pattern = cls.URI_PATTERNS.get(category)
        if not pattern:
            raise ValueError(f"No regex pattern found for category: {category}")

        match = re.match(pattern, uri)
        if not match:
            # Customizing error message to specify what part of the URI is wrong
            raise ValueError(cls._generate_error_message(category, scheme, uri))

        return uri

    @classmethod
    def _generate_error_message(cls, category: str, scheme: str, uri: str):
        """Generates a detailed error message when the URI does not match the pattern."""
        if category == "sql":
            return f"Invalid SQL URI format: {uri}. Expected format: {scheme}://user:password@host:port/dbname"
        elif category == "mongodb":
            return f"Invalid MongoDB URI format: {uri}. Expected format: mongodb://user:password@host:port/auth_dbname?database=dbname"
        elif category == "snowflake":
            return f"Invalid Snowflake URI format: {uri}. Expected format: snowflake://user:password@account/dbname"
        elif category == "motherduck":
            return (
                f"Invalid Motherduck URI format: {uri}. Expected format: md://dbname?token=<token>"
            )
        elif category == "bigquery":
            return f"Invalid Bigquery URI format: {uri}. Expected format: bigquery://projectId?client_id=<client_id>&client_secret=<client_secret>&refresh_token=<refresh_token>"
        elif category == "filebased":
            return (
                f"Invalid file-based URI format: {uri}. Expected format: duckdb://path/to/database"
            )
        elif category == "s3":
            return f"Invalid S3 URI format: {uri}. Expected format: s3://bucket_name?access_key_id=<access_key_id>&access_key_secret=<access_key_secret>&region=<region>"
        elif category == "gcs":
            return f"Invalid GCS URI format: {uri}. Expected format: gs://bucket-name/file_key"
        elif category == "azure":
            return f"Invalid Azure URI format: {uri}. Expected format: az://container-name/blob"
        elif category == "file":
            return f"Invalid file URI format: {uri}. Expected format: file:///path/to/file"
        else:
            return f"Invalid {category} URI format: {uri}. Please check the URI structure."
