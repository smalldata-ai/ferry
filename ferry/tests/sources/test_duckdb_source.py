# import pytest
# from unittest.mock import patch, MagicMock
# import duckdb
# import dlt
# from ferry.src.sources.duckdb_source import DuckDBSource
# from ferry.src.restapi.database_uri_validator import DatabaseURIValidator

# @pytest.fixture
# def duckdb_source():
#     return DuckDBSource("duckdb:///path/to/database.duckdb")

# @patch.object(DatabaseURIValidator, 'validate_uri')
# def test_validate_uri(mock_validate):
#     uri = "duckdb:///path/to/database.duckdb"
#     DuckDBSource(uri)
#     mock_validate.assert_called_once_with(uri)

# @patch.object(duckdb, 'connect')
# def test_dlt_source_system_table_exists(mock_duckdb_connect, duckdb_source):
#     uri = "duckdb:///path/to/database.duckdb"
#     table_name = "test_table"
    
#     mock_conn = MagicMock()
#     mock_duckdb_connect.return_value = mock_conn
#     mock_conn.execute.return_value.fetchall.side_effect = [[(table_name,)], [(1, 'data')]]
    
#     resource = duckdb_source.dlt_source_system(uri, table_name)
    
#     mock_duckdb_connect.assert_called_once_with("path/to/database.duckdb")
#     mock_conn.execute.assert_any_call("SELECT table_name FROM information_schema.tables")
#     mock_conn.execute.assert_any_call(f"SELECT * FROM {table_name}")
    
#     assert callable(resource)

# @patch.object(duckdb, 'connect')
# def test_dlt_source_system_table_not_exists(mock_duckdb_connect, duckdb_source):
#     uri = "duckdb:///path/to/database.duckdb"
#     table_name = "non_existent_table"
    
#     mock_conn = MagicMock()
#     mock_duckdb_connect.return_value = mock_conn
#     mock_conn.execute.return_value.fetchall.side_effect = [[("existing_table",)]]
    
#     with pytest.raises(ValueError, match=f"⚠️ Table '{table_name}' not found in DuckDB!"):
#         duckdb_source.dlt_source_system(uri, table_name)
    
#     mock_duckdb_connect.assert_called_once_with("path/to/database.duckdb")
#     mock_conn.execute.assert_any_call("SELECT table_name FROM information_schema.tables")
