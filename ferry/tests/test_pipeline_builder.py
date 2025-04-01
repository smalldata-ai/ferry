import pytest
from unittest.mock import patch, MagicMock, ANY
from dlt.common.pipeline import LoadInfo
from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.pipeline_builder import PipelineBuilder


@pytest.fixture
def ingest_data():
    return IngestModel(
        identity="test_pipeline",
        source_uri="postgresql://user:password@localhost:5432/mydb",
        destination_uri="clickhouse://user:password@localhost:9000/mydb",
        resources=[{"source_table_name": "source_table"}],
    )


@pytest.fixture
def ingest_data_multiple_sources():
    return IngestModel(
        identity="test_pipeline",
        source_uri="postgresql://user:password@localhost:5432/mydb",
        destination_uri="clickhouse://user:password@localhost:9000/mydb",
        resources=[
            {"source_table_name": "table1"},
            {"source_table_name": "table2"},
            {"source_table_name": "table3"},
        ],
    )


@patch("dlt.pipeline")
def test_build_pipeline(mock_dlt_pipeline, ingest_data):
    """Test if dlt.pipeline is called correctly in build()"""
    mock_pipeline = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline

    builder = PipelineBuilder(ingest_data)
    builder.build()

    print(f"\nExpected call: dlt.pipeline(pipeline_name={ingest_data.identity}, ...)")
    print(f"Actual call: {mock_dlt_pipeline.call_args}")

    mock_dlt_pipeline.assert_called_once_with(
        pipeline_name=ingest_data.identity, dataset_name=ANY, destination=ANY, progress=ANY
    )


def test_build_source_resources(ingest_data):
    """Test if _build_source_resources() correctly calls dlt_source_system"""
    builder = PipelineBuilder(ingest_data)
    builder.source.dlt_source_system = MagicMock(return_value=["mock_resource"])

    assert builder._build_source_resources() == ["mock_resource"]


@patch("dlt.pipeline")
def test_run_pipeline(mock_dlt_pipeline, ingest_data):
    """Test if run() calls dlt_source_system and pipeline.run() correctly"""
    mock_pipeline = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline

    mock_load_info = MagicMock(spec=LoadInfo)
    mock_load_info.metrics = {"rows_loaded": 100}
    mock_pipeline.run.return_value = mock_load_info

    builder = PipelineBuilder(ingest_data)
    builder.pipeline = mock_pipeline

    builder.source.dlt_source_system = MagicMock(
        return_value="mock_resource"
    )  # Return a single string

    builder.source.dlt_source_system.reset_mock()
    builder.run()

    builder.source.dlt_source_system.assert_called_once_with(
        uri=ingest_data.source_uri, resources=ingest_data.resources, identity=ingest_data.identity
    )

    # mock_data = builder.source.dlt_source_system()
    # flattened_data = [
    #     item
    #     for sublist in mock_data
    #     for item in (sublist if isinstance(sublist, list) else [sublist])
    # ]

    mock_pipeline.run.assert_called_once_with(data=["mock_resource"])


@patch("dlt.pipeline")
def test_run_pipeline_multiple_sources(mock_dlt_pipeline, ingest_data_multiple_sources):
    """Test if run() correctly handles multiple source resources"""
    mock_pipeline = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline

    mock_load_info = MagicMock(spec=LoadInfo)
    mock_load_info.metrics = {"rows_loaded": 300}
    mock_pipeline.run.return_value = mock_load_info

    builder = PipelineBuilder(ingest_data_multiple_sources)
    builder.pipeline = mock_pipeline
    builder.source.dlt_source_system = MagicMock(side_effect=lambda **kwargs: kwargs["resources"])

    builder.run()

    print("\nExpected call: pipeline.run()")
    print(f"Actual call: {mock_pipeline.run.call_args}")

    mock_pipeline.run.assert_called_once()
