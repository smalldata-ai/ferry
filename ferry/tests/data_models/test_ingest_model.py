import pytest
from pydantic import ValidationError
from ferry.src.data_models.ingest_model import IngestModel


@pytest.fixture
def valid_ingest_data():
    return {
        "identity": "test_pipeline",
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "resources": [
            {
                "source_table_name": "source_table",
            }
        ],
    }


def test_successfully_ingest_model(valid_ingest_data):
    """Test that a valid LoadDataRequest initializes correctly."""
    request = IngestModel(**valid_ingest_data)
    assert request.source_uri == valid_ingest_data["source_uri"]
    assert request.destination_uri == valid_ingest_data["destination_uri"]
    assert (
        request.resources[0].source_table_name
        == valid_ingest_data["resources"][0]["source_table_name"]
    )


@pytest.mark.parametrize(
    "field, error_message",
    [
        ("source_uri", "URI must be provided"),
        ("destination_uri", "URI must be provided"),
    ],
)
def test_invalid_ingest_data_request_field(valid_ingest_data, field, error_message):
    invalid_data = valid_ingest_data.copy()
    invalid_data[field] = ""

    with pytest.raises(ValidationError) as exc_info:
        IngestModel(**invalid_data)
    assert error_message in str(exc_info.value)


def test_single_resource_should_be_provided(valid_ingest_data):
    invalid_data = valid_ingest_data.copy()
    invalid_data["resources"] = []

    with pytest.raises(ValidationError) as exc_info:
        IngestModel(**invalid_data)
    assert "At least one resource must be provided" in str(exc_info.value)


@pytest.fixture
def valid_ingest_data_with_dataset_and_destination_table():
    return {
        "identity": "test_pipeline",
        "source_uri": "postgresql://user:password@localhost:5432/mydb",
        "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
        "dataset_name": "public",
        "resources": [
            {
                "source_table_name": "source_table",
                "destination_table_name": "source_table",
            }
        ],
    }


def test_successfully_ingest_with_destination_meta(
    valid_ingest_data_with_dataset_and_destination_table,
):
    request = IngestModel(**valid_ingest_data_with_dataset_and_destination_table)
    assert (
        request.resources[0].destination_table_name
        == valid_ingest_data_with_dataset_and_destination_table["resources"][0][
            "destination_table_name"
        ]
    )
    assert (
        request.dataset_name == valid_ingest_data_with_dataset_and_destination_table["dataset_name"]
    )


# @pytest.fixture
# def ingest_data_with_invalid_write_disposition():
#     return {
#         "identity": "test_pipeline",
#         "source_uri": "postgresql://user:password@localhost:5432/mydb",
#         "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
#         "source_table_name": "source_table",
#         "write_disposition": "replac"
#     }

# def test_validate_ingest_model_with_invalid_write_disposition(ingest_data_with_invalid_write_disposition):
#     with pytest.raises(ValidationError) as exc_info:
#         IngestModel(**ingest_data_with_invalid_write_disposition)

#     assert f"Input should be 'replace', 'append' or 'merge'" in str(exc_info.value)

# @pytest.fixture
# def ingest_data_with_replace_write_disposition():
#     return {
#         "identity": "test_pipeline",
#         "source_uri": "postgresql://user:password@localhost:5432/mydb",
#         "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
#         "source_table_name": "source_table",
#         "write_disposition": "replace"
#     }

# def test_validate_ingest_model_with_replace_wd(ingest_data_with_replace_write_disposition):
#     model = IngestModel(**ingest_data_with_replace_write_disposition)
#     assert model.write_disposition.value == ingest_data_with_replace_write_disposition["write_disposition"]


# def test_validate_ingest_model_with_replace_wd_and_strategy(ingest_data_with_replace_write_disposition):
#     ingest_data_with_replace_write_disposition["replace_config"] = {"strategy": "insert-from-staging"}
#     model = IngestModel(**ingest_data_with_replace_write_disposition)
#     assert model.write_disposition.value == ingest_data_with_replace_write_disposition["write_disposition"]
#     assert model.replace_config.strategy.value == "insert-from-staging"


# @pytest.fixture
# def ingest_data_with_append_write_disposition():
#     return {
#         "identity": "test_pipeline",
#         "source_uri": "postgresql://user:password@localhost:5432/mydb",
#         "destination_uri": "clickhouse://user:password@localhost:9000/mydb",
#         "source_table_name": "source_table",
#         "write_disposition": "append",
#     }

# def test_validate_ingest_model_with_append_wd(ingest_data_with_append_write_disposition):
#     model = IngestModel(**ingest_data_with_append_write_disposition)
#     assert model.write_disposition.value == ingest_data_with_append_write_disposition["write_disposition"]
