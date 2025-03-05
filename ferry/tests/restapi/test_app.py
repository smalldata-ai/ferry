import pytest
from pydantic import ValidationError
from fastapi import status
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from ferry.src.pipeline_builder import PipelineBuider
from ferry.src.restapi.models import LoadDataRequest
from ferry.src.restapi.app import app

@pytest.fixture
def valid_ingest_request_data():
    return {
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "source_table_name": "source_table"
    }

@pytest.fixture
def invalid_ingest_request_data():
    return {
        "source_uri": "invalid_uri",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "source_table_name": "source_table",
    }

@pytest.fixture
def client():
    return TestClient(app)

def test_ingest_invalid_request(client, invalid_ingest_request_data):
    response = client.post("/ingest", json=invalid_ingest_request_data)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    response_data = response.json()
    assert response_data == {'errors': {'source_uri': ['Value error, Unsupported URI scheme: ']}}


def test_ingest_data_successfully(client, valid_ingest_request_data):
    with patch.object(PipelineBuider, 'build') as mock_build_method:

        mock_pipeline = MagicMock()
        mock_pipeline.get_name.return_value = "postgres_to_clickhouse"
        mock_pipeline.run.return_value = None
        mock_build_method.return_value = mock_pipeline

        response = client.post("/ingest", json=valid_ingest_request_data)

    assert response.status_code == status.HTTP_200_OK
    response_data = response.json()
    assert response_data["status"] == "success"
    assert response_data["message"] == "Data Ingestion is completed successfully"
    assert response_data["pipeline_name"] == "postgres_to_clickhouse"


def test_ingest_unexpected_error(client, valid_ingest_request_data):
    with patch.object(PipelineBuider, 'build') as mock_run_method:
        mock_run_method.side_effect = Exception("Some unexpected error")
        response = client.post("/ingest", json=valid_ingest_request_data)

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    response_json = response.json()
    assert response_json["status"] == "error"
    assert response_json.get("message") == "An internal server error occured"