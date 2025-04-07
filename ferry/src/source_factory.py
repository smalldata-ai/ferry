from urllib.parse import urlparse
from ferry.src.sources.azure_storage_source import AzureStorageSource
from ferry.src.sources.clickhouse_source import ClickhouseSource
from ferry.src.sources.gcs_source import GCSSource
from ferry.src.sources.local_file_source import LocalFileSource
from ferry.src.sources.mongodb_source import MongoDbSource
from ferry.src.sources.source_base import SourceBase
from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.s3_source import S3Source
from ferry.src.sources.sql_db_source import SqlDbSource  # Import S3Source
from ferry.src.sources.confluent_kafka_source import KafkaSource


class SourceFactory:
    _items = {
        "postgres": SqlDbSource,
        "postgresql": SqlDbSource,
        "duckdb": SqlDbSource,
        "s3": S3Source,
        "sqlite": SqlDbSource,
        "clickhouse": ClickhouseSource,
        "mysql": SqlDbSource,
        "mssql": SqlDbSource,
        "mariadb": SqlDbSource,
        "snowflake": SqlDbSource,
        "mongodb": MongoDbSource,
        "az": AzureStorageSource,
        "gs": GCSSource,
        "file": LocalFileSource,
        "kafka": KafkaSource,
    }

    @staticmethod
    def get(uri: str) -> SourceBase:
        """Get the appropriate source object based on the URI"""
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme in SourceFactory._items:
            class_ = SourceFactory._items.get(parsed_uri.scheme)
            return class_()  # Pass the URI to the class constructor
        else:
            raise InvalidSourceException(f"Invalid source URI scheme: {parsed_uri.scheme}")
