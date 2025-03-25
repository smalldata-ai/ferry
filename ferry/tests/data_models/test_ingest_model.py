import pytest
from pydantic import ValidationError
from ferry.src.data_models.incremental_config_model import IncrementalConfig
from ferry.src.data_models.merge_config_model import MergeConfig, MergeStrategy
from ferry.src.data_models.replace_config_model import ReplaceConfig
from ferry.src.uri_validator import URIValidator
from ferry.src.data_models.ingest_model import ResourceConfig, WriteDispositionType, IngestModel
import pytest
from pydantic import ValidationError


def test_resource_config_valid():
    resource = ResourceConfig(source_table_name="source_table")
    assert resource.source_table_name == "source_table"
    assert resource.write_disposition == WriteDispositionType.REPLACE

def test_resource_config_invalid_source_table_name():
    with pytest.raises(ValidationError, match="Field must be provided"):
        ResourceConfig(source_table_name="")

def test_resource_config_append_with_invalid_config():
    with pytest.raises(ValidationError, match="No config is accepted when write_disposition is 'append'"):
        ResourceConfig(source_table_name="source_table", write_disposition=WriteDispositionType.APPEND, replace_config=ReplaceConfig())

def test_resource_config_merge_without_merge_config():
    with pytest.raises(ValidationError, match="merge_config is required when write_disposition is 'merge'"):
        ResourceConfig(source_table_name="source_table", write_disposition=WriteDispositionType.MERGE)

def test_ingest_model_invalid_uri():
    with pytest.raises(ValidationError, match="URI must be provided"):
        IngestModel(identity="pipeline1", source_uri="", destination_uri="db://dest", resources=[ResourceConfig(source_table_name="source_table")])

def test_ingest_model_no_resources():
    with pytest.raises(ValidationError, match="At least one resource must be provided"):
        IngestModel(identity="pipeline1", source_uri="db://source", destination_uri="db://dest", resources=[])

def test_ingest_model_invalid_identity():
    with pytest.raises(ValidationError, match="Field must be provided"):
        IngestModel(identity="", source_uri="db://source", destination_uri="db://dest", resources=[ResourceConfig(source_table_name="source_table")])