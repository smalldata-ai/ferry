import pytest
from pydantic import ValidationError
from ferry.src.data_models.replace_config_model import ReplaceConfig
@pytest.fixture
def valid_replace_config():
    return {
        "strategy": "truncate-and-insert"
    }

@pytest.mark.parametrize(
    "strategy",
    [
        ("truncate-and-insert"),
        ("insert-from-staging"),
        ("staging-optimized"),
    ],
)
def test_valid_replace_config(valid_replace_config, strategy):
    valid_replace_config = valid_replace_config.copy()
    valid_replace_config["strategy"] = strategy
    config = ReplaceConfig(**valid_replace_config)
    assert config.strategy.value == valid_replace_config["strategy"]


def test_invalid_replace_config(valid_replace_config):
    valid_replace_config = valid_replace_config.copy()
    valid_replace_config["strategy"] = "delete-insert"
    with pytest.raises(ValidationError) as exc_info:
        ReplaceConfig(**valid_replace_config)
    assert " Input should be 'truncate-and-insert'" in str(exc_info.value)
