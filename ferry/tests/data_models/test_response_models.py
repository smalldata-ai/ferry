import pytest
from pydantic import ValidationError
from ferry.src.data_models.response_models import IngestResponse


def test_valid_ingest_response():
    """Test that a valid IngestResponse initializes correctly."""
    data = {
        "status": "success",
        "message": "Data loaded successfully",
        "pipeline_name": "test_pipeline",
    }
    response = IngestResponse(**data)
    assert response.status.value == data["status"]
    assert response.message == data["message"]
    assert response.pipeline_name == data["pipeline_name"]


def test_invalid_ingest_response():
    """Test LoadDataResponse raises ValidationError for invalid status."""
    data = {
        "status": "invalid_status",
        "message": "Some message",
        "pipeline_name": "test_pipeline",
    }
    with pytest.raises(ValidationError):
        IngestResponse(**data)
