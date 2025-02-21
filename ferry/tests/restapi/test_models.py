import pytest
from pydantic import ValidationError
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse  

def test_valid_load_data_request_duckdb():
    request = LoadDataRequest(
        source_uri="duckdb:///path/to/source_db.duckdb",
        destination_uri="duckdb:///path/to/destination_db.duckdb",
        source_table_name="source_table",
        destination_table_name="destination_table",
        dataset_name="test_dataset"
    )
    assert request.source_uri.startswith("duckdb")
    assert request.destination_uri.startswith("duckdb")

def test_invalid_duckdb_source_uri():
    with pytest.raises(ValidationError, match="Source Database must specify a valid DuckDB file path"):
        LoadDataRequest(
            source_uri="duckdb://invalid_path",
            destination_uri="duckdb:///path/to/destination_db.duckdb",
            source_table_name="source_table",
            destination_table_name="destination_table",
            dataset_name="test_dataset"
        )

def test_invalid_duckdb_destination_uri():
    with pytest.raises(ValidationError, match="Destination Database must specify a valid DuckDB file path"):
        LoadDataRequest(
            source_uri="duckdb:///path/to/source_db.duckdb",
            destination_uri="duckdb://invalid_path",
            source_table_name="source_table",
            destination_table_name="destination_table",
            dataset_name="test_dataset"
        )

def test_empty_source_table_name():
    with pytest.raises(ValidationError, match="Field must not be empty"):
        LoadDataRequest(
            source_uri="duckdb:///path/to/source_db.duckdb",
            destination_uri="duckdb:///path/to/destination_db.duckdb",
            source_table_name="",
            destination_table_name="destination_table",
            dataset_name="test_dataset"
        )

def test_valid_load_data_response():
    response = LoadDataResponse(
        status="success",
        message="Data loaded successfully",
        pipeline_name="test_pipeline",
        table_processed="test_table"
    )
    assert response.status == "success"
    assert response.message == "Data loaded successfully"
    assert response.pipeline_name == "test_pipeline"
    assert response.table_processed == "test_table"

def test_invalid_load_data_response_status():
    with pytest.raises(ValidationError, match="Input should be 'success' or 'error'"):
        LoadDataResponse(
            status="failed",
            message="Invalid status test"
        )
