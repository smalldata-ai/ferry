import pytest
from ferry.src.source_factory import SourceFactory
from ferry.src.sources.azure_storage_source import AzureStorageSource
from ferry.src.sources.gcs_source import GCSSource
from ferry.src.sources.local_file_source import LocalFileSource
from ferry.src.sources.mongodb_source import MongoDbSource
from ferry.src.sources.s3_source import S3Source
from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.sql_db_source import SqlDbSource

@pytest.mark.parametrize(
    "uri, expected_class",
    [
        ("postgresql://user:password@localhost:5432/dbname", SqlDbSource),
        ("postgres://user:password@localhost:5432/dbname", SqlDbSource),
        ("duckdb:///path/to/database.db", SqlDbSource),
        ("s3://bucket-name?file_key=data.csv", S3Source),
        ("sqlite:////path/to/database.db", SqlDbSource),
        ("clickhouse://user:password@localhost:9000/dbname", SqlDbSource),
        ("mysql://user:password@localhost:3306/dbname", SqlDbSource),
        ("mssql://user:pass@host/db", SqlDbSource),
        ("mariadb://user:password@localhost:3306/dbname", SqlDbSource),
        ("snowflake://user:password/ferrytest/MY_DATASET", SqlDbSource),
        ("mongodb://user:password@host:port", MongoDbSource),
        ("gs://bucket-name?file_key=data.csv", GCSSource),
        ("az://bucket-name?file_key=data.csv", AzureStorageSource),
        ("file://path/to/file.csv", LocalFileSource),
    ]
)
def test_source_factory(uri, expected_class):
    """Test that the URI returns the expected source instance."""
    source = SourceFactory.get(uri)
    assert isinstance(source, expected_class)

def test_invalid_source():
    """Test that an invalid URI scheme raises an InvalidSourceException."""
    uri = "invalidscheme://user:password@localhost:1234/dbname"
    with pytest.raises(InvalidSourceException, match="Invalid source URI scheme: invalidscheme"):
        SourceFactory.get(uri)
