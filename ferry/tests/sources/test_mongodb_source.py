import pytest
import pymongo
from unittest.mock import patch, MagicMock
from ferry.src.sources.mongodb_source import MongoDbSource
from ferry.src.data_models.ingest_model import ResourceConfig, IncrementalConfig

@pytest.fixture
def mock_mongo_client():
    with patch("pymongo.MongoClient") as mock_client:
        mock_db = MagicMock()
        mock_db.name = "test_db"
        mock_client.return_value.get_database.return_value = mock_db
        yield mock_client

@pytest.fixture
def sample_resource_config():
    return ResourceConfig(
        source_table_name="test_collection",
        destination_table_name="dest_collection",
        column_rules={"exclude": ["password"], "pseudonymize": ["email"]},
        incremental_config=IncrementalConfig(
            incremental_key="timestamp",
            start_position="2024-01-01",
            lag_window=0
        ),
        write_disposition_config=None
    )

@pytest.fixture
def mongodb_source():
    return MongoDbSource()

def test_valid_resource_config(mongodb_source, sample_resource_config, mock_mongo_client):
    uri = "mongodb://localhost:27017/test_db"
    collections = [sample_resource_config]
    source = mongodb_source.dlt_source_system(uri, collections, "test_identity")
    assert source is not None
    assert len(source.resources) == 1

def test_invalid_source_table(mongodb_source, mock_mongo_client):
    with pytest.raises(ValueError, match="Field must be provided"):
        invalid_resource = ResourceConfig(
            source_table_name="",
            destination_table_name="dest_collection"
        )
        uri = "mongodb://localhost:27017/test_db"
        mongodb_source.dlt_source_system(uri, [invalid_resource], "test_identity")

def test_empty_source_table(mongodb_source, mock_mongo_client):
    with pytest.raises(ValueError):
        empty_resource = ResourceConfig(
            source_table_name="missing_collection",
            destination_table_name="dest_collection"
        )
        uri = "mongodb://localhost:27017/test_db"
        mongodb_source.dlt_source_system(uri, [empty_resource], "test_identity")

def test_valid_ingest_model(mongodb_source, sample_resource_config, mock_mongo_client):
    uri = "mongodb://localhost:27017/test_db"
    collections = [sample_resource_config]
    source = mongodb_source.dlt_source_system(uri, collections, "test_identity")
    assert source.schema.name == "test_identity"
    assert len(source.resources) == 1
