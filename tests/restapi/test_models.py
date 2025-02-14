import pytest
from pydantic import ValidationError
from pydantic_core import ValidationError as PydanticCoreValidationError

from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse

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
def valid_load_data_response_data():
    return {
        "status": "success",
        "message": "Data loaded successfully",
        "pipeline_name": "my_pipeline",
        "table_processed": "my_table",
    }

def test_valid_load_data_request(valid_load_data_request_data):
    request = LoadDataRequest(**valid_load_data_request_data)
    assert str(request.source_uri) == valid_load_data_request_data["source_uri"]
    assert str(request.destination_uri) == valid_load_data_request_data["destination_uri"]
    assert request.source_table_name == valid_load_data_request_data["source_table_name"]
    assert request.destination_table_name == valid_load_data_request_data["destination_table_name"]
    assert request.dataset_name == valid_load_data_request_data["dataset_name"]

@pytest.mark.parametrize(
    "field, error_message",
    [
        ("source_uri", "Source URI must be provided"),
        ("destination_uri", "Destination URI must be provided"),
        ("source_table_name", "Source Table Name must be provided"),
        ("destination_table_name", "Destination Table Name must be provided"),
        ("dataset_name", "Dataset Name must be provided"),
    ],
)
def test_invalid_load_data_request_field(valid_load_data_request_data, field, error_message):
    invalid_data = valid_load_data_request_data.copy()
    invalid_data[field] = ""

    with pytest.raises(ValidationError) as exc_info:
        LoadDataRequest(**invalid_data)
        
    assert error_message in str(exc_info.value)

@pytest.mark.parametrize(
    "field",
    [
        "source_uri", "destination_uri", "source_table_name",
        "destination_table_name", "dataset_name"
    ],
)
def test_missing_load_data_request_field(valid_load_data_request_data, field):
    invalid_data = valid_load_data_request_data.copy()
    invalid_data[field] = None

    with pytest.raises(ValidationError) as exc_info:
        LoadDataRequest(**invalid_data)
        
    assert f"Input should be a valid string" in str(exc_info.value)


@pytest.mark.parametrize(
    "invalid_source_uri, expected_error_part",
    [
        ("postgresql://user:password@localhost:5432", "Database name is required"),
        ("postgresql://localhost:5432/mydb", "Username and password are required"),
        ("postgresql://user:password@:5432/mydb", "Host is required"),
        ("postgresql://user:password@localhost/mydb", "Port is required"),
        ("http://user:password@localhost:5432/mydb", "URL scheme should start with postgresql"),
        ("postgresql://user:pass@localhost:5432/", "Database name is required"),
        ("postgresql://user@localhost:5432/mydb", "Password is required"),
        ("postgresql://:password@localhost:5432/mydb", "Username is required"),
        ("postgresql://user:password@localhost:abcd/mydb", "Invalid port"),
        ("postgres://user:password@localhost:5432/mydb", "URL scheme should start with postgresql"),
        ("postgresql+asyncpg://user:password@localhost:5432/mydb", "URL scheme should start with postgresql"),
        ("user:password@localhost:5432/mydb", "URL scheme should start with postgresql"),
        ("postgresql://user:password@localhost:5432", "Database name is required"),
        ("postgresql://user:password@localhost:port/mydb", "Invalid port"),
        ("postgresql://@localhost:5432/mydb", "Username and password are required"),
        ("postgresql://user:password@/mydb", "Host is required"),
        ("postgresql:///mydb", "Username, password, and host are required"),
    ],
)
def test_invalid_postgres_uri_format(invalid_source_uri, expected_error_part, valid_load_data_request_data):
    invalid_data = valid_load_data_request_data.copy()
    invalid_data["source_uri"] = invalid_source_uri

    with pytest.raises((ValidationError, PydanticCoreValidationError)) as exc_info:
        LoadDataRequest(**invalid_data)
    assert expected_error_part in str(exc_info.value)

@pytest.mark.parametrize(
    "invalid_destination_uri, expected_error_part",
    [
        ("clickhouse://user:password@localhost:9000", "Database name is required"),
        ("clickhouse://localhost:9000/mydb", "Username and password are required"),
        ("clickhouse://user:password@:9000/mydb", "Host is required"),
        ("clickhouse://user:password@localhost/mydb", "Port is required"),
        ("http://user:password@localhost:9000/mydb", "URL scheme should start with clickhouse"),
        ("clickhouse://user:pass@localhost:9000/", "Database name is required"),
        ("clickhouse://user@localhost:9000/mydb", "Password is required"),
        ("clickhouse://:password@localhost:9000/mydb", "Username is required"),
        ("clickhouse://user:password@localhost:abcd/mydb", "Invalid port"),
        ("clickhouse+https://user:password@localhost:9000/mydb", "URL scheme should start with clickhouse"),
        ("user:password@localhost:9000/mydb", "URL scheme should start with clickhouse"),
        ("clickhouse://user:password@localhost:9000", "Database name is required"),
        ("clickhouse://user:password@localhost:port/mydb", "Invalid port"),
        ("clickhouse://@localhost:9000/mydb", "Username and password are required"),
        ("clickhouse://user:password@/mydb", "Host is required"),
        ("clickhouse:///mydb", "Username, password, and host are required"),
    ],
)
def test_invalid_clickhouse_uri_format(invalid_destination_uri, expected_error_part, valid_load_data_request_data):
    invalid_data = valid_load_data_request_data.copy()
    invalid_data["destination_uri"] = invalid_destination_uri

    with pytest.raises(ValidationError) as exc_info:
        LoadDataRequest(**invalid_data)
    assert expected_error_part in str(exc_info.value)


def test_valid_load_data_response(valid_load_data_response_data):
    response = LoadDataResponse(**valid_load_data_response_data)
    assert response.status == valid_load_data_response_data["status"]
    assert response.message == valid_load_data_response_data["message"]
    assert response.pipeline_name == valid_load_data_response_data["pipeline_name"]
    assert response.table_processed == valid_load_data_response_data["table_processed"]


def test_load_data_response_error():
    response_data = {
        "status": "error",
        "message": "Failed to load data",
        "pipeline_name": None,
        "table_processed": None,
    }
    response = LoadDataResponse(**response_data)
    assert response.status == "error"
    assert response.message == "Failed to load data"
    assert response.pipeline_name is None
    assert response.table_processed is None

def test_load_data_response_missing_optional_fields():
    response_data = {
        "status": "success",
        "message": "Data loaded successfully",
    }
    response = LoadDataResponse(**response_data) # type: ignore
    assert response.status == "success"
    assert response.message == "Data loaded successfully"
    assert response.pipeline_name is None
    assert response.table_processed is None


def test_invalid_load_data_response():
    with pytest.raises(ValidationError) as exc_info:
        LoadDataResponse(status="invalid", message="Test") # type: ignore
    assert "status" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        LoadDataResponse(status="success") # type: ignore
    assert "message" in str(exc_info.value)