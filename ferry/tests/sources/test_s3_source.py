import unittest
from unittest.mock import patch, MagicMock, ANY
from ferry.src.sources.s3_source import S3Source
from dlt.common.configuration.specs import AwsCredentials

class TestS3Source(unittest.TestCase):

    def setUp(self):
        """Set up a fresh S3Source instance for each test."""
        self.s3_source = S3Source()

    def test_s3source_initialization(self):
        """Test S3Source instantiation."""
        self.assertIsInstance(self.s3_source, S3Source)

    def test_parse_s3_uri(self):
        """Test _parse_s3_uri with a valid S3 URI."""
        uri = "s3://my-bucket/path/to/file.csv?access_key_id=test-key&access_key_secret=test-secret&region=us-east-1"
        bucket_name, aws_credentials = self.s3_source._parse_s3_uri(uri)
        self.assertEqual(bucket_name, "my-bucket")
        self.assertEqual(aws_credentials.aws_access_key_id, "test-key")
        self.assertEqual(aws_credentials.aws_secret_access_key, "test-secret")
        self.assertEqual(aws_credentials.region_name, "us-east-1")

    @patch('ferry.src.sources.s3_source.filesystem')
    def test_create_file_resource_with_correct_parameters(self, mock_filesystem):
        """Test _create_file_resource constructs resource correctly."""
        bucket_name = "test-bucket"
        aws_credentials = AwsCredentials("test-key", "test-secret", None, "us-west-2")
        table_name = "sample.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        result = self.s3_source._create_file_resource(bucket_name, aws_credentials, table_name)
        mock_filesystem.assert_called_once_with("s3://test-bucket", aws_credentials, "sample.csv*")
        mock_resource.apply_hints.assert_called_once_with(incremental=ANY)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.s3_source.filesystem')
    def test_create_file_resource_with_path(self, mock_filesystem):
        """Test _create_file_resource with path in bucket name."""
        bucket_name = "test-bucket/path/to"
        aws_credentials = AwsCredentials("test-key", "test-secret", None, "us-west-2")
        table_name = "sample.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        result = self.s3_source._create_file_resource(bucket_name, aws_credentials, table_name)
        mock_filesystem.assert_called_once_with("s3://test-bucket/path/to", aws_credentials, "sample.csv*")
        mock_resource.apply_hints.assert_called_once_with(incremental=ANY)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.s3_source.dlt.sources.incremental')
    @patch('ferry.src.sources.s3_source.filesystem')
    def test_create_file_resource_incremental_hints(self, mock_filesystem, mock_incremental):
        """Test _create_file_resource applies incremental hints correctly."""
        bucket_name = "test-bucket"
        aws_credentials = AwsCredentials("test-key", "test-secret", None, "us-west-2")
        table_name = "sample.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_incremental.return_value = MagicMock()
        result = self.s3_source._create_file_resource(bucket_name, aws_credentials, table_name)
        mock_incremental.assert_called_once_with("modification_date")
        mock_resource.apply_hints.assert_called_once_with(incremental=mock_incremental.return_value)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.s3_source.filesystem')
    def test_create_file_resource_filesystem_error(self, mock_filesystem):
        """Test _create_file_resource with filesystem failure."""
        mock_filesystem.side_effect = Exception("S3 connection failed")
        with self.assertRaises(Exception) as context:
            self.s3_source._create_file_resource("test-bucket", AwsCredentials("key", "secret", None, "us-west-2"), "test.csv")
        self.assertIn("S3 connection failed", str(context.exception))

    @patch('ferry.src.sources.s3_source.filesystem')
    def test_create_file_resource_empty_table_name(self, mock_filesystem):
        """Test _create_file_resource with empty table_name."""
        bucket_name = "test-bucket"
        aws_credentials = AwsCredentials("key", "secret", None, "us-west-2")
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        result = self.s3_source._create_file_resource(bucket_name, aws_credentials, "")
        mock_filesystem.assert_called_once_with("s3://test-bucket", aws_credentials, "*")
        mock_resource.apply_hints.assert_called_once_with(incremental=ANY)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.s3_source.filesystem')
    def test_create_file_resource_empty_bucket_name(self, mock_filesystem):
        """Test _create_file_resource with empty bucket_name."""
        aws_credentials = AwsCredentials("key", "secret", None, "us-west-2")
        table_name = "test.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        result = self.s3_source._create_file_resource("", aws_credentials, table_name)
        mock_filesystem.assert_called_once_with("s3://", aws_credentials, "test.csv*")
        mock_resource.apply_hints.assert_called_once_with(incremental=ANY)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.s3_source.filesystem')
    def test_create_file_resource_special_chars_in_bucket_name(self, mock_filesystem):
        """Test _create_file_resource with special characters in bucket name."""
        bucket_name = "test-bucket!@#"
        aws_credentials = AwsCredentials("key", "secret", None, "us-west-2")
        table_name = "test.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        result = self.s3_source._create_file_resource(bucket_name, aws_credentials, table_name)
        mock_filesystem.assert_called_once_with("s3://test-bucket!@#", aws_credentials, "test.csv*")
        mock_resource.apply_hints.assert_called_once_with(incremental=ANY)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.s3_source.read_csv')
    def test_apply_reader_csv(self, mock_read_csv):
        """Test _apply_reader for CSV."""
        mock_file_resource = MagicMock()
        mock_read_csv.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "csv_result"
        result = self.s3_source._apply_reader(mock_file_resource, "data.csv")
        mock_read_csv.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_csv.return_value)
        self.assertEqual(result, "csv_result")

    @patch('ferry.src.sources.s3_source.read_jsonl')
    def test_apply_reader_jsonl(self, mock_read_jsonl):
        """Test _apply_reader for JSONL."""
        mock_file_resource = MagicMock()
        mock_read_jsonl.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "jsonl_result"
        result = self.s3_source._apply_reader(mock_file_resource, "data.jsonl")
        mock_read_jsonl.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_jsonl.return_value)
        self.assertEqual(result, "jsonl_result")

    @patch('ferry.src.sources.s3_source.read_parquet')
    def test_apply_reader_parquet(self, mock_read_parquet):
        """Test _apply_reader for Parquet."""
        mock_file_resource = MagicMock()
        mock_read_parquet.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "parquet_result"
        result = self.s3_source._apply_reader(mock_file_resource, "data.parquet")
        mock_read_parquet.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_parquet.return_value)
        self.assertEqual(result, "parquet_result")

    def test_apply_reader_unsupported_format(self):
        """Test _apply_reader raises error for unsupported format."""
        mock_file_resource = MagicMock()
        with self.assertRaises(ValueError) as context:
            self.s3_source._apply_reader(mock_file_resource, "data.txt")
        self.assertIn("Unsupported file format for table: data.txt", str(context.exception))

    @patch('ferry.src.sources.s3_source.read_csv')
    def test_apply_reader_special_chars(self, mock_read_csv):
        """Test _apply_reader with special characters in table_name."""
        mock_file_resource = MagicMock()
        mock_read_csv.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "csv_result"
        result = self.s3_source._apply_reader(mock_file_resource, "data file.csv")
        mock_read_csv.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_csv.return_value)
        self.assertEqual(result, "csv_result")

    @patch('ferry.src.sources.s3_source.read_jsonl')
    def test_apply_reader_mixed_case(self, mock_read_jsonl):
        """Test _apply_reader with mixed-case extension."""
        mock_file_resource = MagicMock()
        mock_read_jsonl.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "jsonl_result"
        result = self.s3_source._apply_reader(mock_file_resource, "data.JsOnL")
        mock_read_jsonl.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_jsonl.return_value)
        self.assertEqual(result, "jsonl_result")

    @patch('ferry.src.sources.s3_source.read_csv')
    @patch('ferry.src.sources.s3_source.filesystem')
    def test_dlt_source_system_csv(self, mock_filesystem, mock_read_csv):
        """Test dlt_source_system for CSV."""
        uri = "s3://test-bucket?access_key_id=abc&access_key_secret=xyz&region=us-west-2"
        table_name = "test.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_csv.return_value = MagicMock()
        mock_resource.__or__.return_value = "csv_resource"
        result = self.s3_source.dlt_source_system(uri, table_name)
        mock_filesystem.assert_called_once_with("s3://test-bucket", ANY, "test.csv*")
        called_args = mock_filesystem.call_args[0]
        credentials = called_args[1]
        self.assertEqual(credentials.aws_access_key_id, "abc")
        self.assertEqual(credentials.aws_secret_access_key, "xyz")
        self.assertEqual(credentials.region_name, "us-west-2")
        mock_read_csv.assert_called_once()
        self.assertEqual(result, "csv_resource")

    @patch('ferry.src.sources.s3_source.read_jsonl')
    @patch('ferry.src.sources.s3_source.filesystem')
    def test_dlt_source_system_jsonl(self, mock_filesystem, mock_read_jsonl):
        """Test dlt_source_system for JSONL."""
        uri = "s3://test-bucket?access_key_id=abc&access_key_secret=xyz&region=us-west-2"
        table_name = "test.jsonl"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_jsonl.return_value = MagicMock()
        mock_resource.__or__.return_value = "jsonl_resource"
        result = self.s3_source.dlt_source_system(uri, table_name)
        mock_filesystem.assert_called_once_with("s3://test-bucket", ANY, "test.jsonl*")
        called_args = mock_filesystem.call_args[0]
        credentials = called_args[1]
        self.assertEqual(credentials.aws_access_key_id, "abc")
        self.assertEqual(credentials.aws_secret_access_key, "xyz")
        self.assertEqual(credentials.region_name, "us-west-2")
        mock_read_jsonl.assert_called_once()
        self.assertEqual(result, "jsonl_resource")

    @patch('ferry.src.sources.s3_source.read_parquet')
    @patch('ferry.src.sources.s3_source.filesystem')
    def test_dlt_source_system_parquet(self, mock_filesystem, mock_read_parquet):
        """Test dlt_source_system for Parquet."""
        uri = "s3://test-bucket?access_key_id=abc&access_key_secret=xyz&region=us-west-2"
        table_name = "test.parquet"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_parquet.return_value = MagicMock()
        mock_resource.__or__.return_value = "parquet_resource"
        result = self.s3_source.dlt_source_system(uri, table_name)
        mock_filesystem.assert_called_once_with("s3://test-bucket", ANY, "test.parquet*")
        called_args = mock_filesystem.call_args[0]
        credentials = called_args[1]
        self.assertEqual(credentials.aws_access_key_id, "abc")
        self.assertEqual(credentials.aws_secret_access_key, "xyz")
        self.assertEqual(credentials.region_name, "us-west-2")
        mock_read_parquet.assert_called_once()
        self.assertEqual(result, "parquet_resource")

    @patch('ferry.src.sources.s3_source.read_csv')
    @patch('ferry.src.sources.s3_source.filesystem')
    def test_dlt_source_system_case_insensitive_extension(self, mock_filesystem, mock_read_csv):
        """Test dlt_source_system with uppercase extension."""
        uri = "s3://test-bucket?access_key_id=abc&access_key_secret=xyz&region=us-west-2"
        table_name = "data.CSV"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_csv.return_value = MagicMock()
        mock_resource.__or__.return_value = "csv_result"
        result = self.s3_source.dlt_source_system(uri, table_name)
        mock_filesystem.assert_called_once_with("s3://test-bucket", ANY, "data.CSV*")
        called_args = mock_filesystem.call_args[0]
        credentials = called_args[1]
        self.assertEqual(credentials.aws_access_key_id, "abc")
        self.assertEqual(credentials.aws_secret_access_key, "xyz")
        self.assertEqual(credentials.region_name, "us-west-2")
        mock_read_csv.assert_called_once()
        self.assertEqual(result, "csv_result")

    @patch('ferry.src.sources.s3_source.filesystem')
    def test_dlt_source_system_unsupported_format_end_to_end(self, mock_filesystem):
        """Test dlt_source_system with unsupported format."""
        uri = "s3://test-bucket?access_key_id=abc&access_key_secret=xyz&region=us-west-2"
        table_name = "test.txt"
        mock_filesystem.return_value = MagicMock()
        with self.assertRaises(ValueError) as context:
            self.s3_source.dlt_source_system(uri, table_name)
        self.assertIn("Unsupported file format for table: test.txt", str(context.exception))

    @patch('ferry.src.sources.s3_source.read_csv')
    @patch('ferry.src.sources.s3_source.filesystem')
    def test_dlt_source_system_with_kwargs(self, mock_filesystem, mock_read_csv):
        """Test dlt_source_system with unused kwargs."""
        uri = "s3://test-bucket?access_key_id=abc&access_key_secret=xyz&region=us-west-2"
        table_name = "test.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_csv.return_value = MagicMock()
        mock_resource.__or__.return_value = "csv_resource"
        result = self.s3_source.dlt_source_system(uri, table_name, foo="bar")
        mock_filesystem.assert_called_once_with("s3://test-bucket", ANY, "test.csv*")
        self.assertEqual(result, "csv_resource")

    @patch('ferry.src.sources.s3_source.filesystem')
    def test_dlt_source_system_permissions_error(self, mock_filesystem):
        """Test dlt_source_system with insufficient permissions."""
        uri = "s3://test-bucket?access_key_id=abc&access_key_secret=xyz&region=us-west-2"
        table_name = "test.csv"
        mock_filesystem.side_effect = Exception("Access Denied")
        with self.assertRaises(Exception) as context:
            self.s3_source.dlt_source_system(uri, table_name)
        self.assertIn("Access Denied", str(context.exception))

if __name__ == "__main__":
    unittest.main()