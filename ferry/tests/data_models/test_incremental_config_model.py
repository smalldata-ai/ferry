import pytest
from ferry.src.data_models.append_config_model import AppendConfig
from ferry.src.data_models.incremental_config_model import IncrementalConfig
@pytest.fixture
def valid_incremental_config():
    return {
        "incremental_key": "id"
    }

def test_valid_incremental_config(valid_incremental_config):
    config = IncrementalConfig(**valid_incremental_config)
    assert config.incremental_key == valid_incremental_config["incremental_key"]


def test_invalid_incremental_config(valid_incremental_config):
    valid_incremental_config = valid_incremental_config.copy()
    valid_incremental_config["incremental_key"] = ""
    with pytest.raises(ValueError) as exc_info:
        AppendConfig(**valid_incremental_config)
    assert "Value error, Field must be provided" in str(exc_info.value)
