import pytest
from unittest.mock import patch, MagicMock

from fastapi import HTTPException

from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.restapi.pipeline_utils import (
    create_pipeline,
    load_data_endpoint,
)

@pytest.fixture
def valid_source_uri():
    return "postgresql://user:password@localhost:5432/mydb"

@pytest.fixture
def valid_destination_uri():
    return "clickhouse://user:password@localhost:9000/mydb"

@pytest.fixture
def valid_load_data_request(
    valid_source_uri,
    valid_destination_uri
):
    return LoadDataRequest(
        source_uri=valid_source_uri,
        destination_uri=valid_destination_uri,
        source_table_name="source_table",
        destination_table_name="destination_table",
        dataset_name="my_dataset",
    )



@patch("ferry.src.restapi.pipeline_utils.DestinationFactory.get")
@patch("dlt.pipeline")
def test_create_pipeline_success(
    mock_dlt_pipeline,
    mock_destination_factory_get,
    valid_destination_uri,
):
    mock_destination = MagicMock()
    mock_destination_factory = MagicMock()
    mock_destination_factory.dlt_target_system.return_value = mock_destination
    mock_destination_factory_get.return_value = mock_destination_factory

    mock_pipeline = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline

    pipeline = create_pipeline("test_pipeline", valid_destination_uri, "test_dataset")

    mock_destination_factory_get.assert_called_once_with(valid_destination_uri)
    mock_destination_factory.dlt_target_system.assert_called_once_with(valid_destination_uri)

    mock_dlt_pipeline.assert_called_once_with(
        pipeline_name="test_pipeline",
        destination=mock_destination,
        dataset_name="test_dataset"
    )
    assert pipeline == mock_pipeline


@patch("ferry.src.restapi.pipeline_utils.DestinationFactory.get")
def test_create_pipeline_failures(
    mock_destination_factory_get,
    valid_destination_uri,
):
    mock_destination_factory_get.side_effect = Exception("Invalid Destination URI")

    with pytest.raises(RuntimeError) as exc_info:
        create_pipeline("test_pipeline", valid_destination_uri, "test_dataset")

    assert "Pipeline creation failed: Invalid Destination URI" in str(exc_info.value)



@patch("ferry.src.restapi.pipeline_utils.create_pipeline")
@patch("ferry.src.restapi.pipeline_utils.SourceFactory.get")
@pytest.mark.asyncio
async def test_load_data_endpoint_success(
    mock_source_factory_get,
    mock_create_pipeline,
    valid_load_data_request
):
    mock_pipeline = MagicMock()
    mock_pipeline.run = MagicMock()
    mock_pipeline.pipeline_name = "postgres_to_clickhouse"

    mock_create_pipeline.return_value = mock_pipeline
    mock_source = MagicMock()
    mock_source_factory = MagicMock()
    mock_source_factory.dlt_source_system.return_value = mock_source
    mock_source_factory_get.return_value = mock_source_factory


    response = await load_data_endpoint(valid_load_data_request)

    assert response == LoadDataResponse(
        status="success",
        message="Data loaded successfully",
        pipeline_name=mock_pipeline.pipeline_name,
        table_processed=valid_load_data_request.destination_table_name,
    )

    mock_create_pipeline.assert_called_once_with("postgres_to_clickhouse", valid_load_data_request.destination_uri, valid_load_data_request.dataset_name)
    mock_source_factory_get.assert_called_once_with(valid_load_data_request.source_uri)
    mock_source_factory.dlt_source_system.assert_called_once_with(valid_load_data_request.source_uri, valid_load_data_request.source_table_name)
    mock_pipeline.run.assert_called_once_with(mock_source, write_disposition="replace")


@patch("ferry.src.restapi.pipeline_utils.create_pipeline", side_effect=RuntimeError("Pipeline failure"))
@pytest.mark.asyncio
async def test_load_data_endpoint_runtime_error(
    valid_load_data_request
):
    with pytest.raises(HTTPException) as exc_info:
        await load_data_endpoint(valid_load_data_request)

    assert exc_info.value.status_code == 500
    assert "A runtime error occurred" in exc_info.value.detail


@patch("ferry.src.restapi.pipeline_utils.create_pipeline", side_effect=Exception("Unexpected error"))
@pytest.mark.asyncio
async def test_load_data_endpoint_general_exception(
    valid_load_data_request
):
    with pytest.raises(HTTPException) as exc_info:
        await load_data_endpoint(valid_load_data_request)

    assert exc_info.value.status_code == 500
    assert "An internal server error occurred" in exc_info.value.detail