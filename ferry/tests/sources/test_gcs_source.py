import pytest
from unittest.mock import MagicMock, patch
from ferry.src.sources.gcs_source import GCSSource
from ferry.src.data_models.ingest_model import ResourceConfig, IncrementalConfig

def mock_gcs_source():
    return GCSSource()

def mock_resources():
    
    return [
        ResourceConfig(source_table_name="table1", incremental_config=None),
        ResourceConfig(
            source_table_name="table2",
            incremental_config=IncrementalConfig(incremental_key="incremental_key")
        )
    ]

@patch("ferry.src.sources.gcs_source.filesystem")
@patch("ferry.src.sources.gcs_source.read_csv")
@patch("ferry.src.sources.gcs_source.read_jsonl")
@patch("ferry.src.sources.gcs_source.read_parquet")
def test_dlt_source_system(mock_read_parquet, mock_read_jsonl, mock_read_csv, mock_filesystem):
    gcs_source = mock_gcs_source()
    resources = mock_resources()

   
    mock_file_resource = MagicMock()
    mock_file_resource.apply_hints = MagicMock()
    mock_file_resource.__or__ = MagicMock()  
    
 
    mock_filesystem.return_value = mock_file_resource
    
    
    csv_data = [{"col1": "val1"}, {"col1": "val2"}]
    jsonl_data = [{"col2": "val3", "incremental_key": "2024-01-01"}, {"col2": "val4"}]
    
    
    mock_file_resource.__or__.side_effect = [csv_data, jsonl_data]

    
    dlt_source = gcs_source.dlt_source_system(
        "gs://bucket/path?project_id=test&private_key=key&client_email=email", 
        resources, 
        "test_identity"
    )

    
    assert dlt_source.section == "gcs_source"
    assert len(dlt_source.resources) == 2
    
    
    assert len(dlt_source.resources[0]) == 2
    

    filtered_data = list(gcs_source._apply_row_incremental(jsonl_data, "incremental_key"))
    assert len(filtered_data) == 1
    assert filtered_data[0]["incremental_key"] == "2024-01-01"

def test_parse_gcs_uri():
    gcs_source = mock_gcs_source()
    bucket, credentials = gcs_source._parse_gcs_uri("gs://test-bucket/?project_id=test&private_key=key&client_email=email")
    assert bucket == "test-bucket"
    assert credentials["project_id"] == "test"
    assert credentials["private_key"] == "key"
    assert credentials["client_email"] == "email"

def test_get_reader():
    gcs_source = mock_gcs_source()
    from dlt.sources.filesystem import read_csv, read_jsonl, read_parquet
    assert gcs_source._get_reader("file.csv") == read_csv
    assert gcs_source._get_reader("file.jsonl") == read_jsonl
    assert gcs_source._get_reader("file.parquet") == read_parquet
    with pytest.raises(ValueError):
        gcs_source._get_reader("file.txt")

def test_apply_row_incremental():
    gcs_source = mock_gcs_source()
    resource_config = ResourceConfig(
        source_table_name="table1",
        incremental_config=IncrementalConfig(incremental_key="incremental_key")
    )
    
    
    data = [
        {"id": 1, "incremental_key": "2024-01-01"},
        {"id": 2},  
        {"id": 3, "incremental_key": "2024-02-01"}
    ]
    
    
    incremental_key = gcs_source._get_row_incremental(resource_config)
    assert incremental_key == "incremental_key"
    
   
    filtered_data = list(gcs_source._apply_row_incremental(data, incremental_key))
   
    assert len(filtered_data) == 2
    assert all("incremental_key" in item for item in filtered_data)
    assert filtered_data[0]["incremental_key"] == "2024-01-01"
    assert filtered_data[1]["incremental_key"] == "2024-02-01"
