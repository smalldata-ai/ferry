import unittest
import pytest
import boto3
import pandas as pd
from moto import mock_aws
from io import BytesIO
from unittest.mock import patch, MagicMock
from ferry.src.sources.s3_source import S3Source
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator
from ferry.src.exceptions import InvalidSourceException

@pytest.fixture
def s3_mock():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        yield s3, bucket_name

@pytest.fixture
def s3_source(s3_mock):
    s3_client, bucket_name = s3_mock
    return S3Source(uri=f"s3://{bucket_name}/test.csv")

class TestS3Source(unittest.TestCase):
    
    @patch.object(DatabaseURIValidator, 'validate_uri')
    @patch('boto3.client')
    def test_s3source_initialization(self, mock_boto_client, mock_validate_uri):
        """Test S3Source initialization with valid URI and S3 client setup."""
        mock_client_instance = MagicMock()
        mock_boto_client.return_value = mock_client_instance
        
        uri = "s3://my-bucket/my-folder/myfile.csv?region=us-east-1&file_key=my-folder/myfile.csv"
        s3_source = S3Source(uri)
        
        mock_validate_uri.assert_called_once_with(uri)
        mock_boto_client.assert_called_once_with("s3", region_name="us-east-1")
        self.assertEqual(s3_source.bucket_name, "my-bucket")
        self.assertEqual(s3_source.file_key, "my-folder/myfile.csv")

    @patch.object(DatabaseURIValidator, 'validate_uri', side_effect=ValueError("Invalid URI"))
    def test_s3source_invalid_uri(self, mock_validate_uri):
        """Test that S3Source raises InvalidSourceException for invalid URI."""
        uri = "invalid-uri"
        with self.assertRaises(InvalidSourceException) as context:
            S3Source(uri)
        
        self.assertIn("Invalid S3 URI", str(context.exception))
        mock_validate_uri.assert_called_once_with(uri)

    @patch('boto3.client')
    def test_read_s3_file(self, mock_boto_client):
        """Test read_s3_file reads data from S3 and yields records."""
        mock_client_instance = MagicMock()
        mock_boto_client.return_value = mock_client_instance
        
        mock_response = {"Body": MagicMock()}
        mock_response["Body"].read.return_value = b"col1,col2\nvalue1,value2\nvalue3,value4"
        
        mock_client_instance.get_object.return_value = mock_response
        
        uri = "s3://test-bucket/test.csv?region=us-east-1&file_key=test.csv"
        s3_source = S3Source(uri)
        
        records = list(s3_source.read_s3_file())
        
        mock_client_instance.get_object.assert_called_once_with(Bucket="test-bucket", Key="test.csv")
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0], {"col1": "value1", "col2": "value2"})
        self.assertEqual(records[1], {"col1": "value3", "col2": "value4"})

@pytest.mark.parametrize("file_format, key", [
    ("csv", "test.csv"),
    ("json", "test.json"),
    ("parquet", "test.parquet")
])
def test_read_s3_files(s3_mock, s3_source, file_format, key):
    s3_client, bucket_name = s3_mock
    
    if file_format == "csv":
        content = "col1,col2\n1,a\n2,b\n3,c"
    elif file_format == "json":
        content = '{"col1": 1, "col2": "a"}\n{"col1": 2, "col2": "b"}\n{"col1": 3, "col2": "c"}'
    elif file_format == "parquet":
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        content = buffer.getvalue()
    
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=content)
    rows = list(s3_source.read_s3_file(f"s3://{bucket_name}/{key}"))
    
    assert len(rows) == 3
    assert rows[0] == {"col1": 1, "col2": "a"}
    assert rows[1] == {"col1": 2, "col2": "b"}
    assert rows[2] == {"col1": 3, "col2": "c"}

def test_invalid_file_format(s3_source):
    with pytest.raises(ValueError, match="Unsupported file format"):
        list(s3_source.read_s3_file("s3://test-bucket/invalid.txt"))

if __name__ == "__main__":
    unittest.main()
