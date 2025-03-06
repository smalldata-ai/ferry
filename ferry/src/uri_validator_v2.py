import re
from urllib.parse import urlparse, parse_qs

class URIValidator:
    """Validates URIs for different database systems using regex."""

    SCHEMES = {
        "sql": {"postgresql", "postgres", "clickhouse", "redshift", "mysql", "hana", "mssql", "mariadb+pymysql", "mariadb"},
        "filebased": {"duckdb", "sqlite"},
        "mongodb": {"mongodb"},
        "snowflake": {"snowflake"},
        "s3": {"s3"},
    }

    # Regex patterns for URI validation
    URI_PATTERNS = {
        "sql": r"^(?P<scheme>\w+)://(?P<user>\w+)(:(?P<password>[^@]+))?@(?P<host>[\w.-]+)(:(?P<port>\d+))?/(?P<db>\w+)",
        "mongodb": r"^mongodb://(?P<user>\w+)(:(?P<password>[^@]+))?@(?P<host>[\w.-]+)(:(?P<port>\d+))?",
        "snowflake": r"^snowflake://(?P<user>\w+)(:(?P<password>[^@]+))?@(?P<account>[\w.-]+)(/(?P<db>\w+))?",
        "filebased": r"^(?P<scheme>\w+)://(?P<path>.+)",
        "s3": r"^s3://(?P<bucket>[\w.-]+)(/(?P<key>.+))?",
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
                return cls._apply_regex_validation(category, uri)

        raise ValueError(f"Unsupported URI scheme: {scheme}")

    @classmethod
    def _apply_regex_validation(cls, category: str, uri: str):
        """Validates the URI using regex matching."""
        pattern = cls.URI_PATTERNS.get(category)
        if not pattern:
            raise ValueError("No regex pattern found for category")

        match = re.match(pattern, uri)
        if not match:
            raise ValueError(f"Invalid {category} URI format")

        groups = match.groupdict()

        # Additional logic for required fields
        if category in {"sql", "mongodb"} and not groups.get("user"):
            raise ValueError(f"{category.capitalize()} URI must contain a username")

        if category == "sql" and not groups.get("db"):
            raise ValueError("SQL URI must contain a database name")

        if category == "s3" and not groups.get("bucket"):
            raise ValueError("S3 URI must contain a bucket name")

        return uri
