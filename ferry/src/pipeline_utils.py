
import os
from urllib.parse import urlparse

class PipelineUtil:
    SCHEMES_CATEGORY = {
        "sql": {"postgresql", "postgres", "clickhouse", "redshift", "mysql", "hana", "mssql", "mariadb+pymysql", "mariadb"},
        "filebased": {"duckdb", "sqlite"},
        "mongodb": {"mongodb"},
        "snowflake": {"snowflake"},
        "filesystem": {"s3", "gs", "az", "file"},
        "motherduck": {"md"},
        "bigquery": {"bigquery"},
    }

    @classmethod
    def generate_identity(cls, uri: str, table_name: str) -> str:
        parsed = urlparse(uri)
        scheme = parsed.scheme.lower()
        
        for category, schemes in cls.SCHEMES_CATEGORY.items():
            if scheme in schemes:
                return cls._generate_identity(category, parsed, table_name)
        
        raise ValueError(f"Unsupported URI scheme: {scheme}")

    @classmethod
    def _generate_identity(cls, category: str, parsed_uri, table_name: str) -> str:
        database_name = cls._extract_database_name(category, parsed_uri)
        database_name = database_name.replace(".", "_")
        database_name = database_name.replace("/", "_")
        table_name = table_name.replace(".", "_")
        table_name = table_name.replace("/", "_")
        return f"{parsed_uri.scheme}_{database_name}_{table_name}"

    @staticmethod
    def _extract_database_name(category: str, parsed_uri) -> str:
        if category in {"sql", "snowflake"}:
            return parsed_uri.path.lstrip('/')
        if category == "filebased":
            return os.path.basename(parsed_uri.path)
        if category == "mongodb":
            return ""
        if category == "filesystem" or category == "motherduck":
            return parsed_uri.hostname or "unknown"
        return "unknown"