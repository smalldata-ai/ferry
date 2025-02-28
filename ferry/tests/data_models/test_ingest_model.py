import pytest
from pydantic import ValidationError
from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse

@pytest.fixture
def valid_ingest_data():
    return {
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "source_table_name": "source_table",
    }

def test_successfully_ingest_model(valid_ingest_data):
    """Test that a valid LoadDataRequest initializes correctly."""
    request = IngestModel(**valid_ingest_data)
    assert request.source_uri == valid_ingest_data["source_uri"]
    assert request.destination_uri == valid_ingest_data["destination_uri"]
    assert request.source_table_name == valid_ingest_data["source_table_name"]

@pytest.mark.parametrize(
    "field, error_message",
    [
        ("source_uri", "URI must be provided"),
        ("destination_uri", "URI must be provided"),
        ("source_table_name", "Field must be provided"),
    ],
)
def test_invalid_ingest_data_request_field(valid_ingest_data, field, error_message):
    invalid_data = valid_ingest_data.copy()
    invalid_data[field] = ""

    with pytest.raises(ValidationError) as exc_info:
        IngestModel(**invalid_data)
    assert error_message in str(exc_info.value)

@pytest.mark.parametrize(
    "field",
    [
        "source_uri", "destination_uri", "source_table_name"
        
    ],
)
def test_missing_load_data_request_field(valid_ingest_data, field):
    invalid_data = valid_ingest_data.copy()
    invalid_data[field] = None

    with pytest.raises(ValidationError) as exc_info:
        IngestModel(**invalid_data)
        
    assert f"Input should be a valid string" in str(exc_info.value)
