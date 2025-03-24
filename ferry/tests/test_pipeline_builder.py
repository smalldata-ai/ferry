import pytest
import unittest
from unittest.mock import patch, MagicMock, ANY

from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.destination_factory import DestinationFactory
from ferry.src.destinations.clickhouse_destination import ClickhouseDestination
from ferry.src.pipeline_builder import PipelineBuilder
from ferry.src.source_factory import SourceFactory
from ferry.src.sources.sql_db_source import SqlDbSource

@pytest.fixture
def ingest_data():
    return {
        "identity": "test_pipeline",
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "resources": [
            {
                "source_table_name": "source_table",
            }
        ]
        

    }
def test_destination_is_initialized(ingest_data):
    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    assert isinstance(builder.destination, ClickhouseDestination)

def test_source_is_initialized(ingest_data):
    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    assert isinstance(builder.source, SqlDbSource)    

@patch.object(SourceFactory, 'get')
@patch.object(DestinationFactory, 'get')
@patch('dlt.pipeline')
def test_build_pipeline(mock_dlt_pipeline, mock_get_destination, mock_get_source, ingest_data):
    """Test creating a DLT pipeline with a valid destination URI."""
    mock_destination = MagicMock()
    mock_get_destination.return_value.dlt_target_system.return_value = mock_destination
    mock_get_destination.return_value.default_schema_name.return_value = "default_schema"

    mock_pipeline = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline

    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    builder.build()
    
    mock_get_destination.assert_called_once_with(model.destination_uri)
    mock_dlt_pipeline.assert_called_once
    # assert builder.destination_table_name == "source_table"
