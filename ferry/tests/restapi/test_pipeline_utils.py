import pytest
from unittest.mock import patch, MagicMock

from fastapi import HTTPException

from dlt.sources.credentials import ConnectionStringCredentials

from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.restapi.pipeline_utils import (
    create_credentials,
    create_pipeline,
    postgres_source,
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

@pytest.fixture
def valid_request():
    return LoadDataRequest(
        source_uri="postgresql://user:password@localhost:5432/mydb",
        destination_uri="clickhouse://user:password@localhost:9000/mydb",
        source_table_name="source_table",
        destination_table_name="destination_table",
        dataset_name="my_dataset",
    )


def test_create_credentials_success(
        valid_source_uri
):
    credentials = create_credentials(valid_source_uri)
    assert isinstance(credentials, ConnectionStringCredentials)
    assert credentials.to_native_representation() == valid_source_uri


@patch("dlt.destinations.clickhouse")
@patch("ferry.src.restapi.pipeline_utils.create_credentials")
@patch("dlt.pipeline")
def test_create_pipeline_success(
    mock_dlt_pipeline,
    mock_create_credentials,
    mock_clickhouse,
    valid_destination_uri,
):
    mock_credentials = MagicMock()
    mock_create_credentials.return_value = mock_credentials
    mock_destination = MagicMock()
    mock_clickhouse.return_value = mock_destination
    mock_pipeline = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline

    pipeline = create_pipeline("test_pipeline", valid_destination_uri, "test_dataset")

    mock_create_credentials.assert_called_once_with(valid_destination_uri)
    mock_clickhouse.assert_called_once_with(mock_credentials)
    mock_dlt_pipeline.assert_called_once_with(
        pipeline_name="test_pipeline",
        destination=mock_destination,
        dataset_name="test_dataset"
    )
    assert pipeline == mock_pipeline


@patch("dlt.destinations.clickhouse")
@patch("ferry.src.restapi.pipeline_utils.create_credentials")
def test_create_pipeline_failures(
    mock_create_credentials,
    mock_clickhouse,
    valid_destination_uri,
):
    mock_create_credentials.side_effect = ValueError("Invalid Destination URI")
    mock_clickhouse.return_value = MagicMock()

    with pytest.raises(RuntimeError) as exc_info:
        create_pipeline("test_pipeline", valid_destination_uri, "test_dataset")

    assert "Pipeline creation failed: Invalid Destination URI" in str(exc_info.value)


@patch("ferry.src.restapi.pipeline_utils.sql_database")
@patch("ferry.src.restapi.pipeline_utils.create_credentials")
def test_postgres_source(
    mock_create_credentials,
    mock_sql_database
):
    source_uri = "test_source_uri"
    table_name = "test_table"
    mock_credentials_instance = MagicMock()
    mock_create_credentials.return_value = mock_credentials_instance
    mock_source = MagicMock()
    mock_sql_database.return_value = mock_source

    postgres_source(source_uri, table_name)

    mock_create_credentials.assert_called_once_with(source_uri)
    mock_sql_database.assert_called_once_with(mock_credentials_instance)
    mock_source.with_resources.assert_called_once_with(table_name)

    with patch("ferry.src.restapi.pipeline_utils.sql_database") as mock_sql_database_fail:
        mock_sql_database_fail.side_effect = Exception("Source creation error")
        with pytest.raises(RuntimeError, match="Source creation failed: Source creation error"):
            postgres_source(source_uri, table_name)


@patch("ferry.src.restapi.pipeline_utils.create_pipeline")
@patch("ferry.src.restapi.pipeline_utils.postgres_source")
@pytest.mark.asyncio
async def test_load_data_endpoint_success(
    mock_postgres_source,
    mock_create_pipeline,
    valid_request
):
    mock_pipeline = MagicMock()
    mock_pipeline.run = MagicMock()
    mock_pipeline.pipeline_name = "postgres_to_clickhouse"

    mock_create_pipeline.return_value = mock_pipeline
    mock_postgres_source.return_value = "mock_source"

    response = await load_data_endpoint(valid_request)

    assert response == LoadDataResponse(
        status="success",
        message="Data loaded successfully",
        pipeline_name=mock_pipeline.pipeline_name,
        table_processed=valid_request.destination_table_name,
    )

    mock_create_pipeline.assert_called_once_with("postgres_to_clickhouse", valid_request.destination_uri, valid_request.dataset_name)
    mock_postgres_source.assert_called_once_with(valid_request.source_uri, valid_request.source_table_name)
    mock_pipeline.run.assert_called_once_with("mock_source", write_disposition="replace")


@patch("ferry.src.restapi.pipeline_utils.create_pipeline", side_effect=RuntimeError("Pipeline failure"))
@pytest.mark.asyncio
async def test_load_data_endpoint_runtime_error(
    valid_request
):
    with pytest.raises(HTTPException) as exc_info:
        await load_data_endpoint(valid_request)

    assert exc_info.value.status_code == 500
    assert "A runtime error occurred" in exc_info.value.detail


@patch("ferry.src.restapi.pipeline_utils.create_pipeline", side_effect=Exception("Unexpected error"))
@pytest.mark.asyncio
async def test_load_data_endpoint_general_exception(
    valid_request
):
    with pytest.raises(HTTPException) as exc_info:
        await load_data_endpoint(valid_request)

    assert exc_info.value.status_code == 500
    assert "An internal server error occurred" in exc_info.value.detail
