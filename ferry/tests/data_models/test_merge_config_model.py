from pydantic import ValidationError
import pytest
from ferry.src.data_models.merge_config_model import MergeConfig

@pytest.fixture
def invalid_merge_config():
    return {
        "strategy": "",
    }

def test_strategy_mandatory_in_merge_config(invalid_merge_config):
    with pytest.raises(ValidationError) as exc_info:
      MergeConfig(**invalid_merge_config)
    assert "Input should be 'delete-insert', 'scd2' or 'upsert'" in str(exc_info.value)

@pytest.fixture
def empty_delete_insert_merge_config():
    return {
        "strategy": "delete-insert",
        
    }
def test_delete_insert_config_mandatory(empty_delete_insert_merge_config):
    with pytest.raises(ValueError) as exc_info:
      MergeConfig(**empty_delete_insert_merge_config)
    assert "delete_insert_config is required" in str(exc_info.value)    

@pytest.fixture
def valid_delete_insert_merge_config():
    return {
        "strategy": "delete-insert",
        "delete_insert_config": {
            "merge_key": "batch_id",
            "primary_key": ("id","name"),
            "hard_delete_column": "deleted_at",
            "dedup_sort_column": {"ep_id": "asc"}
        }
    }

def test_valid_delete_insert_merge_config(valid_delete_insert_merge_config):
    config = MergeConfig(**valid_delete_insert_merge_config)
    assert config.strategy.value == valid_delete_insert_merge_config["strategy"]
    assert config.delete_insert_config.merge_key == valid_delete_insert_merge_config["delete_insert_config"]["merge_key"]
    assert config.delete_insert_config.primary_key == valid_delete_insert_merge_config["delete_insert_config"]["primary_key"]
    assert config.delete_insert_config.hard_delete_column == valid_delete_insert_merge_config["delete_insert_config"]["hard_delete_column"]
    assert config.delete_insert_config.dedup_sort_column["ep_id"].value == valid_delete_insert_merge_config["delete_insert_config"]["dedup_sort_column"]["ep_id"]

def test_delete_insert_config_key_mandatory(valid_delete_insert_merge_config):
    invalid_data = valid_delete_insert_merge_config.copy()
    invalid_data["delete_insert_config"]["merge_key"] = None
    invalid_data["delete_insert_config"]["primary_key"] = None
    with pytest.raises(ValueError) as exc_info:
      MergeConfig(**invalid_data)
    assert "At least one of 'primary' or 'merge'" in str(exc_info.value)        


@pytest.fixture
def empty_upsert_merge_config():
    return {
        "strategy": "upsert",
        
    }
def test_upsert_config_mandatory(empty_upsert_merge_config):
    with pytest.raises(ValueError) as exc_info:
      MergeConfig(**empty_upsert_merge_config)
    assert "upsert_config is required" in str(exc_info.value)    

@pytest.fixture
def valid_upsert_config():
    return {
      "strategy": "upsert",
      "upsert_config": {
          "primary_key": ("id","name"),
          "hard_delete_column": "deleted_at",
    }}

def test_successfully_ingest_with_upsert(valid_upsert_config):
    config = MergeConfig(**valid_upsert_config)
    assert config.strategy.value == valid_upsert_config["strategy"]
    assert config.upsert_config.primary_key == valid_upsert_config["upsert_config"]["primary_key"]
    assert config.upsert_config.hard_delete_column == valid_upsert_config["upsert_config"]["hard_delete_column"]

def test_upsert_config_mandatory(valid_upsert_config):
    invalid_data = valid_upsert_config.copy()
    invalid_data["upsert_config"] = None
    with pytest.raises(ValueError) as exc_info:
      MergeConfig(**invalid_data)
    assert "upsert_config is required" in str(exc_info.value)    

def test_upsert_config_key_mandatory(valid_upsert_config):
    invalid_data = valid_upsert_config.copy()
    invalid_data["upsert_config"]["primary_key"] = ""
    with pytest.raises(ValueError) as exc_info:
      MergeConfig(**invalid_data)
    assert "Primary key must be provided" in str(exc_info.value)                