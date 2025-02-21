import pytest
from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.postgres_source import PostgresSource
from ferry.src.source_factory import SourceFactory
from ferry.src.sources.s3_source import S3Source  # Import the S3 source class


def test_get_s3_source():
    """Test if S3 source is correctly returned."""
    uri = "s3://my-bucket/path/to/data.csv"
    source = SourceFactory.get(uri)
    assert isinstance(source, S3Source)


def test_get_postgres_source():
    uri = 'postgres://username:password@localhost/dbname'
    source = SourceFactory.get(uri)
    assert isinstance(source, PostgresSource)


def test_get_invalid_source():
    """Test if an invalid source raises an exception."""
    uri = "invalid://some_path"
    with pytest.raises(InvalidSourceException):
        SourceFactory.get(uri)




