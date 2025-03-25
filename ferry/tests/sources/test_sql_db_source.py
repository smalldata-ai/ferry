import pytest
import dlt
import logging
from unittest.mock import patch, MagicMock
from ferry.src.sources.sql_db_source import SqlDbSource
from ferry.src.data_models.ingest_model import ResourceConfig
from dlt.extract.source import DltSource
from dlt.sources.sql_database import sql_database

logger = logging.getLogger(__name__)

@pytest.fixture
def sql_db_source():
    return SqlDbSource()

@pytest.fixture
def mock_resource_config():
    return ResourceConfig(source_table_name="test_table")

@patch("ferry.src.sources.sql_db_source.SqlDbSource.create_credentials")
@patch("ferry.src.sources.sql_db_source.sql_database")
def test_dlt_source_system(mock_sql_database, mock_create_credentials, sql_db_source, mock_resource_config):
    uri = "postgres://user:password@localhost:5432/dbname"
    identity = "test_identity"
    mock_credentials = MagicMock()
    mock_create_credentials.return_value = mock_credentials
    
    mock_sql_source = MagicMock()
    mock_sql_database.return_value = mock_sql_source
    
    mock_data_iterator = MagicMock()
    mock_sql_source.with_resources.return_value = mock_data_iterator
    
    source = sql_db_source.dlt_source_system(uri, [mock_resource_config], identity)
    
    assert isinstance(source, DltSource)
    assert source.schema.name == identity
    assert source.section == "sql_db_source"
    assert len(source.resources) == 1

@patch("ferry.src.sources.sql_db_source.sql_database")
def test_dlt_source_system_no_resources(mock_sql_database, sql_db_source):
    uri = "postgres://user:password@localhost:5432/dbname"
    identity = "test_identity"
    
    mock_sql_source = MagicMock()
    mock_sql_database.return_value = mock_sql_source
    
    source = sql_db_source.dlt_source_system(uri, [], identity)
    
    assert isinstance(source, DltSource)
    assert source.schema.name == identity
    assert source.section == "sql_db_source"
    assert len(source.resources) == 0

@patch("ferry.src.sources.sql_db_source.SqlDbSource.create_credentials")
def test_create_credentials(mock_create_credentials, sql_db_source):
    uri = "postgres://user:password@localhost:5432/dbname"
    mock_create_credentials.return_value = "mock_credentials"
    
    credentials = sql_db_source.create_credentials(uri)
    
    assert credentials == "mock_credentials"
    mock_create_credentials.assert_called_once_with(uri)
