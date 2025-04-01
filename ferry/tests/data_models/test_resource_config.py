import pytest
from pydantic import ValidationError

from ferry.src.data_models.ingest_model import ResourceConfig, WriteDispositionConfig, WriteDispositionType
from ferry.src.data_models.replace_config_model import ReplaceStrategy
from ferry.src.data_models.merge_config_model import MergeStrategy


def test_valid_resource_config():
    config = ResourceConfig(
        source_table_name="users",
        destination_table_name="users_backup",
        write_disposition_config=WriteDispositionConfig(type=WriteDispositionType.REPLACE.value)
    )
    assert config.source_table_name == "users"
    assert config.get_destination_table_name() == "users_backup"

def test_resource_config_without_destination():
    config = ResourceConfig(
        source_table_name="orders",
        write_disposition_config=WriteDispositionConfig(type=WriteDispositionType.APPEND.value)
    )
    assert config.get_destination_table_name() == "orders"

def test_invalid_empty_source_table():
    with pytest.raises(ValidationError):
        ResourceConfig(source_table_name="")

def test_build_wd_config_replace():
    config = ResourceConfig(
        source_table_name="products",
        write_disposition_config=WriteDispositionConfig(type=WriteDispositionType.REPLACE.value)
    )
    wd_config = config.build_wd_config()
    assert wd_config["disposition"] == WriteDispositionType.REPLACE.value
    assert wd_config["strategy"] == ReplaceStrategy.TRUNCATE_INSERT.value

def test_build_wd_config_append():
    config = ResourceConfig(
        source_table_name="sales",
        write_disposition_config=WriteDispositionConfig(type=WriteDispositionType.APPEND.value)
    )
    wd_config = config.build_wd_config()
    assert wd_config == WriteDispositionType.APPEND.value

def test_invalid_append_with_strategy():
    with pytest.raises(ValidationError, match="No strategy or config is accepted when write_disposition type is 'append'"):
        WriteDispositionConfig(type=WriteDispositionType.APPEND.value, strategy="invalid-strategy")

def test_invalid_append_with_config():
    with pytest.raises(ValidationError, match="No strategy or config is accepted when write_disposition type is 'append'"):
        WriteDispositionConfig(type=WriteDispositionType.APPEND.value, config={"some": "value"})

def test_default_write_disposition():
    config = ResourceConfig(source_table_name="customers")
    wd_config = config.build_wd_config()
    assert wd_config == WriteDispositionType.REPLACE.value

def test_merge_strategy_config():
    config = ResourceConfig(
        source_table_name="transactions",
        write_disposition_config=WriteDispositionConfig(type=WriteDispositionType.MERGE.value)
    )
    wd_config = config.build_wd_config()
    assert wd_config["disposition"] == WriteDispositionType.MERGE.value
    assert wd_config["strategy"] == MergeStrategy.DELETE_INSERT.value

def test_merge_scd2_strategy():
    merge_config = {"key_column": "id", "updated_at_column": "last_modified"}
    config = ResourceConfig(
        source_table_name="orders",
        write_disposition_config=WriteDispositionConfig(
            type=WriteDispositionType.MERGE.value, strategy=MergeStrategy.SCD2.value, config=merge_config
        )
    )
    wd_config = config.build_wd_config()
    assert wd_config["disposition"] == WriteDispositionType.MERGE.value
    assert wd_config["strategy"] == MergeStrategy.SCD2.value
    assert wd_config["key_column"] == "id"
    assert wd_config["updated_at_column"] == "last_modified"


def test_invalid_write_disposition_type():
    with pytest.raises(ValidationError) as exc_info:
        WriteDispositionConfig(type="invalid-type")

    error_message = str(exc_info.value)
    print(f"Actual Error Message: {error_message}")  # Debugging step
    
    assert "value is not a valid enumeration member" in error_message or "not a valid WriteDispositionType"


def test_invalid_merge_strategy():
    with pytest.raises(ValidationError, match="Invalid merge strategy"):
        WriteDispositionConfig(type=WriteDispositionType.MERGE.value, strategy="invalid-strategy")

def test_invalid_replace_strategy():
    with pytest.raises(ValidationError, match="Invalid replace strategy"):
        WriteDispositionConfig(type=WriteDispositionType.REPLACE.value, strategy="invalid-strategy")


