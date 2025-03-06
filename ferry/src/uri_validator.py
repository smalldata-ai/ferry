from urllib.parse import urlparse, parse_qs

class URIValidator:
    """Validates URIs: PostgreSQL, DuckDB, S3."""
    SQL_SCHEMES = ["postgresql","postgres","clickhouse","redshift","mysql","hana","mssql","mariadb+pymysql","mariadb"]
    FILEBASED_SCHEMES = ["duckdb","sqlite"]


    @classmethod
    def validate_uri(cls, v: str) -> str:
        """Validates both source and destination URIs."""
        if not v:
            raise ValueError("URI must be provided")

        parsed = urlparse(v)
        scheme = parsed.scheme.lower()
        if scheme in URIValidator.SQL_SCHEMES:
            return cls._validate_sqldb_uri(v, scheme)
        elif scheme in URIValidator.FILEBASED_SCHEMES:
            return cls._validate_filebased_uri(v, scheme)
        elif scheme == "mongodb":
            return cls._validate_mongodb_uri(v)
        elif scheme == "snowflake":
            return cls._validate_snowflake_uri(v)
        elif scheme == "s3":
            return cls._validate_s3_uri(v)
        else:
            raise ValueError(f"Unsupported URI scheme: {scheme}")

    @classmethod
    def _validate_sqldb_uri(cls, v: str, scheme: str) -> str:
        """Validates Sql Db URI."""
        parsed = urlparse(v)

        if parsed.scheme != scheme:
            raise ValueError(f"URI must start with '{scheme}://'")

        if "@" not in (parsed.netloc or ""):
            raise ValueError("URI must contain username and password")

        userinfo, hostport = parsed.netloc.split("@", 1)
        username, _, password = userinfo.partition(":")
        
        if not username:
            raise ValueError("URI must contain a non-empty username")
        
        if ":" not in hostport:
            raise ValueError("URI must specify a host and port")
        
        host, port = hostport.split(":", 1)
        
        if not host:
            raise ValueError("URI must specify a non-empty host")

        if not port.isdigit():
            raise ValueError("URI port must be an integer")

        if not parsed.path or parsed.path == "/":
            raise ValueError("URI must contain a database name")

        return v
    
    @classmethod
    def _validate_mongodb_uri(cls, v: str) -> str:
        """Validates Mongo Db URI."""
        parsed = urlparse(v)

        if parsed.scheme != "mongodb":
            raise ValueError(f"URI must start with 'mongodb://'")

        if "@" not in (parsed.netloc or ""):
            raise ValueError("URI must contain username and password")

        userinfo, hostport = parsed.netloc.split("@", 1)
        username, _, password = userinfo.partition(":")
        
        if not username:
            raise ValueError("URI must contain a non-empty username")
        
        if ":" not in hostport:
            raise ValueError("URI must specify a host and port")
        
        host, port = hostport.split(":", 1)
        
        if not host:
            raise ValueError("URI must specify a non-empty host")

        if not port.isdigit():
            raise ValueError("URI port must be an integer")

        return v
    
    @classmethod
    def _validate_snowflake_uri(cls, v: str) -> str:
        """Validates Snowflake URI."""
        parsed = urlparse(v)

        if parsed.scheme != "snowflake":
            raise ValueError(f"URI must start with 'snowflake://'")

        if "@" not in (parsed.netloc or ""):
            raise ValueError("URI must contain username and password")

        userinfo, account = parsed.netloc.split("@", 1)
        username, _, password = userinfo.partition(":")
        
        if not username:
            raise ValueError("URI must contain a non-empty username")
        
        if not account:
            raise ValueError("URI must specify a non-empty account")
        
        if not parsed.path or parsed.path == "/":
            raise ValueError("URI must contain a database name")

        return v

    @classmethod
    def _validate_filebased_uri(cls, v: str, scheme: str) -> str:
        """Validates Filebased URI."""
        parsed = urlparse(v)

        if parsed.scheme != scheme:
            raise ValueError(f"URI must start with '{scheme}://'")

        if not parsed.path or parsed.path == "/":
            raise ValueError("URI must specify a database file path")

        return v

    @classmethod
    def _validate_s3_uri(cls, v: str) -> str:
        """Validates S3 URI."""
        parsed = urlparse(v)

        if parsed.scheme != "s3":
            raise ValueError("S3 URI must start with 's3://'")

        if not parsed.hostname:
            raise ValueError("S3 URI must include a bucket name")

        query_params = parse_qs(parsed.query)

        if "access_key_id" not in query_params:
            raise ValueError("S3 URI must include a 'access_key_id' parameter in the query string")
        if "access_key_secret" not in query_params:
            raise ValueError("S3 URI must include a 'access_key_secret' parameter in the query string")
        if "region" not in query_params:
            raise ValueError("S3 URI must include a 'access_key_region' parameter in the query string")

        return v
    