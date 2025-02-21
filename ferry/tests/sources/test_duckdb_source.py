import pytest
import duckdb
from unittest.mock import patch, MagicMock
from ferry.src.sources.duckdb_source import DuckDBSource

def test_dlt_source_system_success():
    uri = "duckdb:///test_db.duckdb"
    table_name = "test_table"
    
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = [("test_table",)]
    
    with patch("duckdb.connect", return_value=mock_conn):
        mock_conn.execute.return_value.fetchdf.return_value.to_dict.return_value = [{"id": 1, "name": "test"}]
        source = DuckDBSource()
        resource = source.dlt_source_system(uri, table_name)
        
        assert resource.name == table_name
        assert list(resource) == [{"id": 1, "name": "test"}]

def test_dlt_source_system_table_not_found():
    uri = "duckdb:///test_db.duckdb"
    table_name = "missing_table"
    
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = [("other_table",)]
    
    with patch("duckdb.connect", return_value=mock_conn):
        source = DuckDBSource()
        with pytest.raises(ValueError, match=f"⚠️ Table '{table_name}' not found in DuckDB!"):
            source.dlt_source_system(uri, table_name)
