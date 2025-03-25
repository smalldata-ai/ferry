import pytest
import unittest
from unittest.mock import patch, MagicMock, ANY
from dlt.common.pipeline import LoadInfo
from pydantic import ValidationError

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
        "resources": [{"source_table_name": "source_table"}]
    }

@pytest.fixture
def ingest_data_multiple_tables():
    return {
        "identity": "test_pipeline",
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "resources": [
            {"source_table_name": "table1"},
            {"source_table_name": "table2"},
            {"source_table_name": "table3"}
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
    mock_destination = MagicMock()
    mock_get_destination.return_value.dlt_target_system.return_value = mock_destination
    mock_get_destination.return_value.default_schema_name.return_value = "default_schema"

    mock_pipeline = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline

    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    builder.build()

    mock_get_destination.assert_called_once_with(model.destination_uri)
    mock_dlt_pipeline.assert_called_once()

@patch.object(PipelineBuilder, '_build_source_resources', return_value=["mock_resource"])
@patch('dlt.pipeline')
def test_run_pipeline(mock_dlt_pipeline, mock_build_source, ingest_data):
    mock_pipeline = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline

    mock_load_info = MagicMock(spec=LoadInfo)
    mock_load_info.metrics = {"rows_loaded": 100}
    mock_load_info.load_packages = ["package1"]

    mock_pipeline.run.return_value = mock_load_info

    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    builder.build()
    builder.run()

    mock_pipeline.run.assert_called_once_with(data=["mock_resource"])

def test_invalid_ingest_data():
    invalid_data = {
        "identity": "test_pipeline",
        "source_uri": "postgresql://user:password@localhost:5432/mydb"
    }
    with pytest.raises(ValidationError):
        IngestModel(**invalid_data)

def test_multiple_source_tables(ingest_data_multiple_tables):
    model = IngestModel(**ingest_data_multiple_tables)
    builder = PipelineBuilder(model)
    expected_tables = ["table1", "table2", "table3"]
    actual_tables = [resource.source_table_name for resource in builder.model.resources]
    assert actual_tables == expected_tables

@patch.object(PipelineBuilder, '_build_source_resources', return_value=["mock_resource"])
@patch('dlt.pipeline')
def test_run_pipeline_failure(mock_dlt_pipeline, mock_build_source, ingest_data):
    mock_pipeline = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline
    mock_pipeline.run.side_effect = Exception("Pipeline execution failed")

    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    builder.build()

    with pytest.raises(Exception, match="Pipeline execution failed"):
        builder.run()

def test_repr(ingest_data):
    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    expected_repr = ("DataPipeline(source_uri=postgresql://user:password@localhost:5432/mydb, "
                     "destination_uri=clickhouse://user:password@localhost:9000/mydb, "
                     "source_tables=['source_table']")
    assert builder.__repr__().startswith(expected_repr)

@patch.object(SourceFactory, 'get', side_effect=Exception("Source initialization failed"))
def test_pipeline_initialization_failure(mock_get_source, ingest_data):
    with pytest.raises(Exception, match="Source initialization failed"):
        model = IngestModel(**ingest_data)
        PipelineBuilder(model)

@patch.object(PipelineBuilder, 'build', side_effect=RuntimeError("Pipeline build failed"))
def test_build_pipeline_failure(mock_build, ingest_data):
    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    with pytest.raises(RuntimeError, match="Pipeline build failed"):
        builder.build()

def test_build_source_resources(ingest_data):
    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    builder.source.dlt_source_system = MagicMock(return_value=["mock_resource"])
    assert builder._build_source_resources() == ["mock_resource"]

def test_get_pipeline_name(ingest_data):
    model = IngestModel(**ingest_data)
    builder = PipelineBuilder(model)
    builder.pipeline = MagicMock()
    builder.pipeline.pipeline_name = "test_pipeline"
    assert builder.get_name() == "test_pipeline"
