import pytest
from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.source_base import SourceBase
from ferry.src.sources.duckdb_source import DuckDBSource
from ferry.src.sources.postgres_source import PostgresSource
from ferry.src.source_factory import SourceFactory

def test_get_duckdb_source():
    uri = "duckdb:///path/to/database.duckdb"
    source = SourceFactory.get(uri)
    assert isinstance(source, DuckDBSource)

def test_get_postgres_source():
    uri = "postgresql://user:password@localhost:5432/mydb"
    source = SourceFactory.get(uri)
    assert isinstance(source, PostgresSource)

def test_get_invalid_source():
    uri = "invalid://some_path"
    with pytest.raises(InvalidSourceException):
        SourceFactory.get(uri)
