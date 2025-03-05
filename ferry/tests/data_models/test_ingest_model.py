import pytest
from pydantic import ValidationError
from ferry.src.data_models.ingest_model import IngestModel

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

@pytest.fixture
def valid_ingest_data_with_destination_meta():
    return {
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "source_table_name": "source_table",
        "destination_meta": {
            "table_name": "source_table",
            "dataset_name": "public"
        }
    }

def test_successfully_ingest_with_destination_meta(valid_ingest_data_with_destination_meta):
    request = IngestModel(**valid_ingest_data_with_destination_meta)
    assert request.destination_meta.table_name == valid_ingest_data_with_destination_meta["destination_meta"]["table_name"]
    assert request.destination_meta.dataset_name == valid_ingest_data_with_destination_meta["destination_meta"]["dataset_name"]

@pytest.fixture
def ingest_data_with_invalid_write_disposition():
    return {
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "source_table_name": "source_table",
        "write_disposition": "replac"
    }

def test_validate_ingest_model_with_invalid_write_disposition(ingest_data_with_invalid_write_disposition):
    with pytest.raises(ValidationError) as exc_info:
        IngestModel(**ingest_data_with_invalid_write_disposition)
        
    assert f"Input should be 'replace', 'append' or 'merge'" in str(exc_info.value)

@pytest.fixture
def ingest_data_with_replace_write_disposition():
    return {
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "source_table_name": "source_table",
        "write_disposition": "replace"
    }

def test_validate_ingest_model_with_replace_wd(ingest_data_with_replace_write_disposition):
    model = IngestModel(**ingest_data_with_replace_write_disposition)
    assert model.write_disposition.value == ingest_data_with_replace_write_disposition["write_disposition"]

    
def test_validate_ingest_model_with_replace_wd_and_strategy(ingest_data_with_replace_write_disposition):
    ingest_data_with_replace_write_disposition["replace_config"] = {"strategy": "insert-from-staging"}
    model = IngestModel(**ingest_data_with_replace_write_disposition)
    assert model.write_disposition.value == ingest_data_with_replace_write_disposition["write_disposition"]
    assert model.replace_config.strategy.value == "insert-from-staging"

    
@pytest.fixture
def ingest_data_with_append_write_disposition():
    return {
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "source_table_name": "source_table",
        "write_disposition": "append",
    }

def test_validate_ingest_model_with_append_wd(ingest_data_with_append_write_disposition):
    model = IngestModel(**ingest_data_with_append_write_disposition)
    assert model.write_disposition.value == ingest_data_with_append_write_disposition["write_disposition"]
    