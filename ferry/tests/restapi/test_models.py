import pytest
from pydantic import ValidationError
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse

def test_valid_load_data_request():
    """Test that a valid LoadDataRequest initializes correctly."""
    data = {
        "source_uri": "postgresql://user:pass@localhost:5432/db",
        "destination_uri": "duckdb:///mydb",
        "source_table_name": "source_table",
        "destination_table_name": "destination_table",
        "dataset_name": "my_dataset"
    }
    request = LoadDataRequest(**data)
    assert request.source_uri == data["source_uri"]
    assert request.destination_uri == data["destination_uri"]
    assert request.source_table_name == data["source_table_name"]
    assert request.destination_table_name == data["destination_table_name"]
    assert request.dataset_name == data["dataset_name"]

def test_invalid_load_data_request_missing_uri():
    """Test LoadDataRequest raises ValidationError if URI fields are missing."""
    data = {
        "source_uri": "",
        "destination_uri": "duckdb:///mydb",
        "source_table_name": "source_table",
        "destination_table_name": "destination_table",
        "dataset_name": "my_dataset"
    }
    with pytest.raises(ValidationError, match="URI must be provided"):
        LoadDataRequest(**data)

def test_invalid_load_data_request_empty_table_name():
    """Test LoadDataRequest raises ValidationError if table names or dataset names are empty."""
    data = {
        "source_uri": "postgresql://user:pass@localhost:5432/db",
        "destination_uri": "duckdb:///mydb",
        "source_table_name": "",
        "destination_table_name": "destination_table",
        "dataset_name": "my_dataset"
    }
    with pytest.raises(ValidationError, match="Field must not be empty"):
        LoadDataRequest(**data)

def test_valid_load_data_response():
    """Test that a valid LoadDataResponse initializes correctly."""
    data = {
        "status": "success",
        "message": "Data loaded successfully",
        "pipeline_name": "test_pipeline",
        "table_processed": "test_table"
    }
    response = LoadDataResponse(**data)
    assert response.status == data["status"]
    assert response.message == data["message"]
    assert response.pipeline_name == data["pipeline_name"]
    assert response.table_processed == data["table_processed"]

def test_invalid_load_data_response_status():
    """Test LoadDataResponse raises ValidationError for invalid status."""
    data = {
        "status": "invalid_status",
        "message": "Some message",
        "pipeline_name": "test_pipeline",
        "table_processed": "test_table"
    }
    with pytest.raises(ValidationError):
        LoadDataResponse(**data)