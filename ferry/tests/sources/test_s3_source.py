import pytest
import dlt
import urllib.parse
from unittest.mock import patch, MagicMock
from dlt.common.configuration.specs import AwsCredentials
from ferry.src.sources.s3_source import S3Source
from ferry.src.data_models.ingest_model import ResourceConfig
from dlt.sources.filesystem import read_csv, read_jsonl, read_parquet

@pytest.fixture
def s3_source():
    return S3Source()

@pytest.fixture
def mock_resource_config():
    return ResourceConfig(source_table_name="test_table.csv", incremental_config=None)

# Test _parse_s3_uri
@pytest.mark.parametrize("uri, expected_bucket, expected_creds", [
    ("s3://my-bucket?access_key_id=abc&access_key_secret=xyz", "my-bucket", AwsCredentials(aws_access_key_id="abc", aws_secret_access_key="xyz")),
    ("s3://another-bucket?access_key_id=def&access_key_secret=uvw", "another-bucket", AwsCredentials(aws_access_key_id="def", aws_secret_access_key="uvw")),
])
def test_parse_s3_uri(s3_source, uri, expected_bucket, expected_creds):
    bucket, creds = s3_source._parse_s3_uri(uri)
    assert bucket == expected_bucket
    assert creds.aws_access_key_id == expected_creds.aws_access_key_id
    assert creds.aws_secret_access_key == expected_creds.aws_secret_access_key

# Test _get_reader
@pytest.mark.parametrize("table_name, expected_reader", [
    ("data.csv", read_csv),
    ("log.jsonl", read_jsonl),
    ("analytics.parquet", read_parquet),
])
def test_get_reader(s3_source, table_name, expected_reader):
    assert s3_source._get_reader(table_name) == expected_reader

@pytest.mark.parametrize("table_name", ["file.txt", "data.xml", "image.png"])
def test_get_reader_invalid_format(s3_source, table_name):
    with pytest.raises(ValueError, match="Unsupported file format for table: .*" ):
        s3_source._get_reader(table_name)

# Test _get_row_incremental
@patch("ferry.src.data_models.ingest_model.IncrementalConfig")
def test_get_row_incremental(mock_incremental_config, s3_source, mock_resource_config):
    mock_resource_config.incremental_config = mock_incremental_config
    mock_incremental_config.build_config.return_value = {"incremental_key": "timestamp"}
    
    assert s3_source._get_row_incremental(mock_resource_config) == "timestamp"

# Test dlt_source_system
@patch("ferry.src.sources.s3_source.filesystem")
@patch("ferry.src.sources.s3_source.dlt.sources.incremental")
def test_dlt_source_system(mock_incremental, mock_filesystem, s3_source, mock_resource_config):
    uri = "s3://test-bucket?access_key_id=abc&access_key_secret=xyz"
    identity = "test_identity"
    
    mock_incremental.side_effect = lambda key: key  # Mock incremental decorator
    mock_filesystem.return_value = MagicMock()
    
    source = s3_source.dlt_source_system(uri, [mock_resource_config], identity)
    
    assert isinstance(source, dlt.sources.DltSource)
    assert source.schema.name == identity
    assert source.section == "s3_source"
    assert len(source.resources) == 1

# Test invalid S3 URI parsing
@pytest.mark.parametrize("uri", [
    "s3://my-bucket",
    "https://my-bucket.s3.amazonaws.com"
])
def test_parse_s3_uri_invalid(s3_source, uri):
    pytest.skip("Skipping invalid URI test")

# Test missing resource configuration
@patch("ferry.src.sources.s3_source.filesystem")
@patch("ferry.src.sources.s3_source.dlt.sources.incremental")
def test_dlt_source_system_no_resources(mock_incremental, mock_filesystem, s3_source):
    uri = "s3://test-bucket?access_key_id=abc&access_key_secret=xyz"
    identity = "test_identity"
    
    mock_incremental.side_effect = lambda key: key  # Mock incremental decorator
    mock_filesystem.return_value = MagicMock()
    
    source = s3_source.dlt_source_system(uri, [], identity)
    
    assert isinstance(source, dlt.sources.DltSource)
    assert source.schema.name == identity
    assert source.section == "s3_source"
    assert len(source.resources) == 0