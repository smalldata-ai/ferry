import unittest
from unittest.mock import patch, MagicMock
from ferry.src.sources.s3_source import S3Source
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator
from ferry.src.exceptions import InvalidSourceException

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
    @patch.object(S3Source, 'read_s3_file', return_value=[{"col1": "value1"}])
    @patch('dlt.resource')
    def test_dlt_source_system(self, mock_dlt_resource, mock_read_s3_file, mock_boto_client):
        """Test dlt_source_system ensures data from S3 is converted into a DLT resource."""
        
        mock_dlt_resource.return_value = "dlt_mock_resource"

        uri = "s3://test-bucket/test.csv?region=us-west-2&file_key=test.csv"
        s3_source = S3Source(uri)

        result = s3_source.dlt_source_system(uri, "test_table")

        mock_read_s3_file.assert_called_once()
        mock_dlt_resource.assert_called_once_with(mock_read_s3_file.return_value, name="test_table")
        self.assertEqual(result, "dlt_mock_resource")

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

if __name__ == "__main__":
    unittest.main()
