import pytest
import dlt
from unittest.mock import patch, MagicMock
from pydantic import ValidationError
from datetime import datetime
from ferry.src.sources.mongodb_source import MongoDbSource
from ferry.src.data_models.ingest_model import ResourceConfig, IncrementalConfig, WriteDispositionConfig, WriteDispositionType
from ferry.src.data_models.merge_config_model import MergeStrategy
from ferry.src.data_models.replace_config_model import ReplaceStrategy


@pytest.fixture
def mock_mongo_client(sample_documents):
    """Mock MongoDB client to return dynamic sample documents."""
    with patch("pymongo.MongoClient") as mock_client:
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_collection.find.return_value = sample_documents
        mock_db.__getitem__.return_value = mock_collection
        mock_client.return_value.get_database.return_value = mock_db
        yield mock_client


@pytest.fixture
def sample_resource_config():
    """Provide a sample valid ResourceConfig."""
    return ResourceConfig(
        source_table_name="test_collection",
        destination_table_name="dest_collection",
        column_rules={"exclude": ["password"], "pseudonymize": ["email"]},
        incremental_config=IncrementalConfig(
            incremental_key="timestamp",
            start_position="2024-01-01",
            lag_window=0
        ),
        write_disposition_config=WriteDispositionConfig(
            type=WriteDispositionType.MERGE,
            strategy=MergeStrategy.DELETE_INSERT.value
        )
    )


@pytest.fixture
def sample_documents(sample_resource_config):
    """Generate dynamic sample documents using resource config."""
    key = sample_resource_config.incremental_config.incremental_key
    return [
        {
            "_id": i,
            key: datetime(2024, 1, 1), 
            "name": f"user_{i}",
            "email": f"user_{i}@example.com",
            "identity": "test_identity",
            "uri": "mongodb://localhost:27017/test_db"
        }
        for i in range(3)
    ]



@pytest.fixture
def mongodb_source():
    return MongoDbSource()


def test_valid_resource_config(mongodb_source, sample_resource_config, mock_mongo_client):
    """Test valid resource configuration."""
    uri = "mongodb://localhost:27017/test_db"
    collections = [sample_resource_config]
    identity = "test_identity"

    source_func = mongodb_source.dlt_source_system(uri, collections, identity)

    assert callable(source_func)

    source_data = source_func()  

    resource_data = []
    for resource in source_data.resources.values():
        resource_data.extend(list(resource()))

    assert isinstance(resource_data, list)
    assert len(source_data.resources) == 1

    for doc in resource_data:
        assert doc["identity"] == identity
        assert doc["uri"] == uri
        assert sample_resource_config.incremental_config.incremental_key in doc


def test_invalid_source_table(mongodb_source, mock_mongo_client):
    """Test invalid source table name raises validation error."""
    with pytest.raises(ValidationError, match="Field must be provided"):
        _ = ResourceConfig(
            source_table_name="",  # Invalid
            destination_table_name="dest_collection",
            write_disposition_config=WriteDispositionConfig(
                type=WriteDispositionType.REPLACE,
                strategy=ReplaceStrategy.TRUNCATE_INSERT.value
            )
        )


@patch("ferry.src.sources.mongodb_source.pymongo.MongoClient")
@patch("dlt.pipeline")
def test_valid_ingest_model(mock_pipeline_class, mock_mongo_client_class, mongodb_source, sample_resource_config, sample_documents):
    uri = "mongodb://localhost:27017/test_db"
    collections = [sample_resource_config]
    identity = "test_identity"

    mock_collection = MagicMock()
    mock_collection.find.return_value = sample_documents

    mock_db = MagicMock()
    mock_db.__getitem__.return_value = mock_collection

    mock_mongo_client = MagicMock()
    mock_mongo_client.__getitem__.return_value = mock_db
    mock_mongo_client_class.return_value = mock_mongo_client

    mock_pipeline = MagicMock()
    mock_pipeline_class.return_value = mock_pipeline

    source_func = mongodb_source.dlt_source_system(uri, collections, identity)
    data = source_func()

    resource_data = []
    for resource in data.resources.values():
        resource_data.extend(list(resource()))

    assert len(resource_data) == len(sample_documents)
    for i, doc in enumerate(resource_data):
        assert doc["_id"] == sample_documents[i]["_id"]
        assert doc["email"] == sample_documents[i]["email"]
        assert doc["identity"] == identity
        assert doc["uri"] == uri
