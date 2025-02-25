import pytest
from unittest.mock import patch
from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.postgres_source import PostgresSource
from ferry.src.source_factory import SourceFactory
from ferry.src.sources.s3_source import S3Source  # Import the S3 source class


def test_get_s3_source():
    """Test if S3 source is correctly returned."""
    uri = "s3://my-bucket/path/to/data.csv"
    
    with patch("ferry.src.sources.s3_source.boto3.client") as mock_s3_client:
        source = SourceFactory.get(uri)
        assert isinstance(source, S3Source)
        mock_s3_client.assert_called_once()  # Ensure boto3 client is called


def test_get_postgres_source():
    """Test if Postgres source is correctly returned."""
    uri = "postgres://username:password@localhost/dbname"
    source = SourceFactory.get(uri)
    assert isinstance(source, PostgresSource)


def test_get_invalid_source():
    """Test if an invalid source raises an exception."""
    uri = "invalid://some_path"
    with pytest.raises(InvalidSourceException):
        SourceFactory.get(uri)


def test_source_factory_logging(caplog):
    """Test if the SourceFactory logs correct information."""
    uri = "s3://my-bucket/path/to/data.csv"
    with caplog.at_level("INFO"):
        with patch("ferry.src.sources.s3_source.boto3.client"):
            SourceFactory.get(uri)
    assert "Checking source factory for scheme: s3" in caplog.text
