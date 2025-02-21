import pytest
from unittest.mock import patch, MagicMock
import dlt
from ferry.src.destinations.duckdb_destination import DuckDBDestination

def test_dlt_target_system_valid_uri():
    destination = DuckDBDestination()
    uri = "duckdb:///test_db.duckdb"
    
    with patch("dlt.destinations.duckdb") as mock_duckdb:
        mock_duckdb.return_value = "mock_duckdb_instance"
        result = destination.dlt_target_system(uri)
        
        assert result == "mock_duckdb_instance"
        mock_duckdb.assert_called_once_with(configuration={"database": "test_db.duckdb"})

def test_dlt_target_system_invalid_uri():
    destination = DuckDBDestination()
    invalid_uri = "invalid_scheme:///test_db.duckdb"
    
    with patch("dlt.destinations.duckdb") as mock_duckdb:
        mock_duckdb.side_effect = ValueError("Invalid DuckDB URI")
        
        with pytest.raises(ValueError, match="Invalid DuckDB URI"):
            destination.dlt_target_system(invalid_uri)