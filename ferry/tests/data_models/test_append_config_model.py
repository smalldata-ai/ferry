import pytest
from ferry.src.data_models.append_config_model import AppendConfig
@pytest.fixture
def valid_append_config():
    return {
        "incremental_key": "id"
    }

def test_valid_replace_config(valid_append_config):
    config = AppendConfig(**valid_append_config)
    assert config.incremental_key == valid_append_config["incremental_key"]


def test_invalid_append_config(valid_append_config):
    valid_append_config = valid_append_config.copy()
    valid_append_config["incremental_key"] = ""
    with pytest.raises(ValueError) as exc_info:
        AppendConfig(**valid_append_config)
    assert "Value error, Field must be provided" in str(exc_info.value)
