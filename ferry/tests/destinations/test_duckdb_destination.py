import pytest
from unittest.mock import patch, MagicMock
from ferry.src.destinations.duckdb_destination import DuckDBDestination
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator
import dlt

@pytest.fixture
def duckdb_destination():
    return DuckDBDestination()

@patch.object(DatabaseURIValidator, 'validate_uri')
def test_validate_uri(mock_validate, duckdb_destination):
    uri = "duckdb:///path/to/database.duckdb"
    duckdb_destination.validate_uri(uri)
    mock_validate.assert_called_once_with(uri)

@patch.object(DatabaseURIValidator, 'validate_uri')
@patch('dlt.destinations.duckdb')
def test_dlt_target_system(mock_dlt_duckdb, mock_validate, duckdb_destination):
    uri = "duckdb:///path/to/database.duckdb"
    mock_dlt_instance = MagicMock()
    mock_dlt_duckdb.return_value = mock_dlt_instance

    result = duckdb_destination.dlt_target_system(uri, param1='value1')
    
    mock_validate.assert_called_once_with(uri)
    mock_dlt_duckdb.assert_called_once_with(configuration={"database": "path/to/database.duckdb"}, param1='value1')
    assert result == mock_dlt_instance

@patch.object(DatabaseURIValidator, 'validate_uri', side_effect=ValueError("Invalid URI"))
def test_validate_uri_invalid(mock_validate, duckdb_destination):
    uri = "invalid_uri"
    with pytest.raises(ValueError, match="Invalid URI"):
        duckdb_destination.validate_uri(uri)
    mock_validate.assert_called_once_with(uri)

@patch.object(DatabaseURIValidator, 'validate_uri', side_effect=ValueError("Invalid URI"))
def test_dlt_target_system_invalid_uri(mock_validate, duckdb_destination):
    uri = "invalid_uri"
    with pytest.raises(ValueError, match="Invalid URI"):
        duckdb_destination.dlt_target_system(uri)
    mock_validate.assert_called_once_with(uri)