import pytest
import hashlib
from unittest.mock import MagicMock, patch
from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig
from dlt.sources.credentials import ConnectionStringCredentials
import dlt

class TestSourceBase(SourceBase):
    def dlt_source_system(self, uri, resources, identity):
        pass  # Abstract method, not needed for unit testing

@pytest.fixture
def source_base():
    return TestSourceBase()

@pytest.fixture
def mock_resource_config():
    return ResourceConfig(source_table_name="test_table")

def test_create_credentials(source_base):
    uri = "postgres://user:password@localhost:5432/dbname"
    credentials = source_base.create_credentials(uri)
    assert isinstance(credentials, ConnectionStringCredentials)
    assert hasattr(credentials, "to_native_credentials")  # Ensure correct method is available

def test_pseudonymize_columns(source_base):
    row = {"name": "John Doe", "email": "johndoe@example.com"}
    pseudonymized = source_base._pseudonymize_columns(row, ["email"])
    
    assert pseudonymized["name"] == "John Doe"
    assert pseudonymized["email"] != "johndoe@example.com"
    assert isinstance(pseudonymized["email"], str)
    assert len(pseudonymized["email"]) == 64  # SHA-256 hash length

def test_create_dlt_resource(source_base, mock_resource_config):
    data_iterator = iter([
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"}
    ])

    mock_resource_config.column_rules = {
        "exclude_columns": ["name"],
        "pseudonymizing_columns": ["email"]
    }

    resource = source_base._create_dlt_resource(mock_resource_config, data_iterator)
    processed_data = list(resource)
    
    assert len(processed_data) == 2
    assert "name" not in processed_data[0]  # Ensure exclusion
    assert len(processed_data[0]["email"]) == 64  # SHA-256 hash
    assert "id" in processed_data[0]  # Ensure ID is not removed

def test_create_dlt_resource_no_exclusions(source_base, mock_resource_config):
    data_iterator = iter([
        {"id": 1, "name": "Alice", "email": "alice@example.com"}
    ])
    
    mock_resource_config.column_rules = {}  # No exclusions or pseudonymizing
    
    resource = source_base._create_dlt_resource(mock_resource_config, data_iterator)
    processed_data = list(resource)
    
    assert len(processed_data) == 1
    assert processed_data[0]["name"] == "Alice"
    assert processed_data[0]["email"] == "alice@example.com"

def test_create_dlt_resource_with_empty_iterator(source_base, mock_resource_config):
    data_iterator = iter([])  # Empty iterator
    
    resource = source_base._create_dlt_resource(mock_resource_config, data_iterator)
    processed_data = list(resource)
    
    assert len(processed_data) == 0  # Ensure no data is processed

def test_pseudonymize_columns_no_matching_columns(source_base):
    row = {"name": "John Doe", "email": "johndoe@example.com"}
    pseudonymized = source_base._pseudonymize_columns(row, ["phone"])
    
    assert pseudonymized == row  # Ensure no change if column not in row