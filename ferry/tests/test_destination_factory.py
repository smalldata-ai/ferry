import pytest
from ferry.src.exceptions import InvalidDestinationException
from ferry.src.destinations.duckdb_destination import DuckDBDestination
from ferry.src.destinations.postgres_destination import PostgresDestination
from ferry.src.destinations.clickhouse_destination import ClickhouseDestination
from ferry.src.destination_factory import DestinationFactory

def test_get_duckdb_destination():
    uri = "duckdb:///path/to/database.duckdb"
    destination = DestinationFactory.get(uri)
    assert isinstance(destination, DuckDBDestination)

def test_get_postgres_destination():
    uri = "postgresql://user:password@localhost:5432/mydb"
    destination = DestinationFactory.get(uri)
    assert isinstance(destination, PostgresDestination)

def test_get_clickhouse_destination():
    uri = "clickhouse://user:password@localhost:8123/mydb"
    destination = DestinationFactory.get(uri)
    assert isinstance(destination, ClickhouseDestination)

def test_get_invalid_destination():
    uri = "invalid://some_path"
    with pytest.raises(InvalidDestinationException):
        DestinationFactory.get(uri)
