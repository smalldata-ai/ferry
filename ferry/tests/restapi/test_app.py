from pydantic import ValidationError
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from ferry.src.restapi.models import LoadDataRequest
from ferry.src.restapi.app import app

@pytest.fixture
def valid_load_data_request_data():
    return {
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "source_table_name": "source_table",
        "destination_table_name": "destination_table",
        "dataset_name": "my_dataset",
    }

@pytest.fixture
def invalid_load_data_request_data():
    return {
        "source_uri": "invalid_uri",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "source_table_name": "source_table",
        "destination_table_name": "destination_table",
        "dataset_name": "my_dataset",
    }

@pytest.fixture
def client():
    return TestClient(app)

def test_read_root(client):
    response = client.get("/")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"message": "Hello, World!"}

def test_load_data_success(client, valid_load_data_request_data):
    with patch("ferry.src.restapi.pipeline_utils.create_pipeline") as mock_create_pipeline, \
            patch("ferry.src.restapi.pipeline_utils.postgres_source") as mock_postgres_source:

        mock_pipeline = MagicMock()
        mock_pipeline.pipeline_name = "postgres_to_clickhouse"
        mock_pipeline.run.return_value = None

        mock_create_pipeline.return_value = mock_pipeline
        mock_postgres_source.return_value = "mock_source"
        response = client.post("/load-data", json=valid_load_data_request_data)

    assert response.status_code == status.HTTP_200_OK
    response_data = response.json()
    assert response_data["status"] == "success"
    assert response_data["message"] == "Data loaded successfully"
    assert response_data["pipeline_name"] == "postgres_to_clickhouse"
    assert response_data["table_processed"] == valid_load_data_request_data["destination_table_name"]

def test_invalid_load_data_request(invalid_load_data_request_data):
    with pytest.raises(ValidationError) as exc_info:
        LoadDataRequest(**invalid_load_data_request_data)

    assert "URL scheme should start with postgresql" in str(exc_info.value)

def test_load_data_runtime_error(client, valid_load_data_request_data):
    with patch("ferry.src.restapi.pipeline_utils.create_pipeline") as mock_create_pipeline:
        mock_create_pipeline.side_effect = RuntimeError("Pipeline creation failed")

        response = client.post("/load-data", json=valid_load_data_request_data)

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    response_json = response.json()
    assert response_json["status"] == "error"
    assert response_json.get("message") == "A runtime error occurred"

def test_load_data_unexpected_error(client, valid_load_data_request_data):
    with patch("ferry.src.restapi.pipeline_utils.create_pipeline") as mock_create_pipeline:
        mock_create_pipeline.side_effect = Exception("Some unexpected error")

        response = client.post("/load-data", json=valid_load_data_request_data)

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    response_json = response.json()
    assert response_json["status"] == "error"
    assert response_json.get("message") == "An internal server error occurred"