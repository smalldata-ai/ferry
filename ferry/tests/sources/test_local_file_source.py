import unittest
from unittest.mock import patch, MagicMock, ANY
from ferry.src.sources.local_file_source import LocalFileSource

class TestLocalFileSource(unittest.TestCase):

    def setUp(self):
        """Set up a fresh LocalFileSource instance for each test."""
        self.local_source = LocalFileSource()

    def test_localfile_initialization(self):
        """Test LocalFileSource instantiation."""
        self.assertIsInstance(self.local_source, LocalFileSource)

    def test_parse_local_uri(self):
        """Test _parse_local_uri with a valid local file URI."""
        uri = "file:///path/to/directory"
        base_path = self.local_source._parse_local_uri(uri)
        self.assertEqual(base_path, "/path/to/directory")

    def test_parse_local_uri_invalid_scheme(self):
        """Test _parse_local_uri with an invalid URI scheme."""
        uri = "http:///path/to/directory"
        with self.assertRaises(ValueError) as context:
            self.local_source._parse_local_uri(uri)
        self.assertIn("Unsupported URI scheme for local filesystem: http:///path/to/directory", str(context.exception))

    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_create_file_resource_with_correct_parameters(self, mock_filesystem):
        """Test _create_file_resource constructs resource correctly."""
        base_path = "/path/to/directory"
        table_name = "sample.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        result = self.local_source._create_file_resource(base_path, table_name)
        mock_filesystem.assert_called_once_with(bucket_url="/path/to/directory", file_glob="sample.csv*")
        mock_resource.apply_hints.assert_called_once_with(incremental=ANY)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.local_file_source.dlt.sources.incremental')
    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_create_file_resource_incremental_hints(self, mock_filesystem, mock_incremental):
        """Test _create_file_resource applies incremental hints correctly."""
        base_path = "/path/to/directory"
        table_name = "sample.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_incremental.return_value = MagicMock()
        result = self.local_source._create_file_resource(base_path, table_name)
        mock_incremental.assert_called_once_with("modification_date")
        mock_resource.apply_hints.assert_called_once_with(incremental=mock_incremental.return_value)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_create_file_resource_filesystem_error(self, mock_filesystem):
        """Test _create_file_resource with filesystem failure."""
        mock_filesystem.side_effect = Exception("Local file access failed")
        with self.assertRaises(Exception) as context:
            self.local_source._create_file_resource("/path/to/nonexistent", "test.csv")
        self.assertIn("Local file access failed", str(context.exception))

    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_create_file_resource_empty_table_name(self, mock_filesystem):
        """Test _create_file_resource with empty table_name."""
        base_path = "/path/to/directory"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        result = self.local_source._create_file_resource(base_path, "")
        mock_filesystem.assert_called_once_with(bucket_url="/path/to/directory", file_glob="*")
        mock_resource.apply_hints.assert_called_once_with(incremental=ANY)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_create_file_resource_empty_base_path(self, mock_filesystem):
        """Test _create_file_resource with empty base_path."""
        table_name = "test.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        result = self.local_source._create_file_resource("", table_name)
        mock_filesystem.assert_called_once_with(bucket_url="", file_glob="test.csv*")
        mock_resource.apply_hints.assert_called_once_with(incremental=ANY)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_create_file_resource_special_chars_in_base_path(self, mock_filesystem):
        """Test _create_file_resource with special characters in base path."""
        base_path = "/path/to/dir!@#"
        table_name = "test.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        result = self.local_source._create_file_resource(base_path, table_name)
        mock_filesystem.assert_called_once_with(bucket_url="/path/to/dir!@#", file_glob="test.csv*")
        mock_resource.apply_hints.assert_called_once_with(incremental=ANY)
        self.assertEqual(result, mock_resource)

    @patch('ferry.src.sources.local_file_source.read_csv')
    def test_apply_reader_csv(self, mock_read_csv):
        """Test _apply_reader for CSV."""
        mock_file_resource = MagicMock()
        mock_read_csv.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "csv_result"
        result = self.local_source._apply_reader(mock_file_resource, "data.csv")
        mock_read_csv.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_csv.return_value)
        self.assertEqual(result, "csv_result")

    @patch('ferry.src.sources.local_file_source.read_jsonl')
    def test_apply_reader_jsonl(self, mock_read_jsonl):
        """Test _apply_reader for JSONL."""
        mock_file_resource = MagicMock()
        mock_read_jsonl.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "jsonl_result"
        result = self.local_source._apply_reader(mock_file_resource, "data.jsonl")
        mock_read_jsonl.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_jsonl.return_value)
        self.assertEqual(result, "jsonl_result")

    @patch('ferry.src.sources.local_file_source.read_parquet')
    def test_apply_reader_parquet(self, mock_read_parquet):
        """Test _apply_reader for Parquet."""
        mock_file_resource = MagicMock()
        mock_read_parquet.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "parquet_result"
        result = self.local_source._apply_reader(mock_file_resource, "data.parquet")
        mock_read_parquet.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_parquet.return_value)
        self.assertEqual(result, "parquet_result")

    def test_apply_reader_unsupported_format(self):
        """Test _apply_reader raises error for unsupported format."""
        mock_file_resource = MagicMock()
        with self.assertRaises(ValueError) as context:
            self.local_source._apply_reader(mock_file_resource, "data.txt")
        self.assertIn("Unsupported file format for table: data.txt", str(context.exception))

    @patch('ferry.src.sources.local_file_source.read_csv')
    def test_apply_reader_special_chars(self, mock_read_csv):
        """Test _apply_reader with special characters in table_name."""
        mock_file_resource = MagicMock()
        mock_read_csv.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "csv_result"
        result = self.local_source._apply_reader(mock_file_resource, "data file.csv")
        mock_read_csv.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_csv.return_value)
        self.assertEqual(result, "csv_result")

    @patch('ferry.src.sources.local_file_source.read_jsonl')
    def test_apply_reader_mixed_case(self, mock_read_jsonl):
        """Test _apply_reader with mixed-case extension."""
        mock_file_resource = MagicMock()
        mock_read_jsonl.return_value = MagicMock()
        mock_file_resource.__or__.return_value = "jsonl_result"
        result = self.local_source._apply_reader(mock_file_resource, "data.JsOnL")
        mock_read_jsonl.assert_called_once()
        mock_file_resource.__or__.assert_called_once_with(mock_read_jsonl.return_value)
        self.assertEqual(result, "jsonl_result")

    @patch('ferry.src.sources.local_file_source.read_csv')
    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_dlt_source_system_csv(self, mock_filesystem, mock_read_csv):
        """Test dlt_source_system for CSV."""
        uri = "file:///path/to/directory"
        table_name = "test.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_csv.return_value = MagicMock()
        mock_resource.__or__.return_value = "csv_resource"
        result = self.local_source.dlt_source_system(uri, table_name)
        mock_filesystem.assert_called_once_with(bucket_url="/path/to/directory", file_glob="test.csv*")
        mock_read_csv.assert_called_once()
        self.assertEqual(result, "csv_resource")

    @patch('ferry.src.sources.local_file_source.read_jsonl')
    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_dlt_source_system_jsonl(self, mock_filesystem, mock_read_jsonl):
        """Test dlt_source_system for JSONL."""
        uri = "file:///path/to/directory"
        table_name = "test.jsonl"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_jsonl.return_value = MagicMock()
        mock_resource.__or__.return_value = "jsonl_resource"
        result = self.local_source.dlt_source_system(uri, table_name)
        mock_filesystem.assert_called_once_with(bucket_url="/path/to/directory", file_glob="test.jsonl*")
        mock_read_jsonl.assert_called_once()
        self.assertEqual(result, "jsonl_resource")

    @patch('ferry.src.sources.local_file_source.read_parquet')
    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_dlt_source_system_parquet(self, mock_filesystem, mock_read_parquet):
        """Test dlt_source_system for Parquet."""
        uri = "file:///path/to/directory"
        table_name = "test.parquet"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_parquet.return_value = MagicMock()
        mock_resource.__or__.return_value = "parquet_resource"
        result = self.local_source.dlt_source_system(uri, table_name)
        mock_filesystem.assert_called_once_with(bucket_url="/path/to/directory", file_glob="test.parquet*")
        mock_read_parquet.assert_called_once()
        self.assertEqual(result, "parquet_resource")

    @patch('ferry.src.sources.local_file_source.read_csv')
    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_dlt_source_system_case_insensitive_extension(self, mock_filesystem, mock_read_csv):
        """Test dlt_source_system with uppercase extension."""
        uri = "file:///path/to/directory"
        table_name = "data.CSV"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_csv.return_value = MagicMock()
        mock_resource.__or__.return_value = "csv_result"
        result = self.local_source.dlt_source_system(uri, table_name)
        mock_filesystem.assert_called_once_with(bucket_url="/path/to/directory", file_glob="data.CSV*")
        mock_read_csv.assert_called_once()
        self.assertEqual(result, "csv_result")

    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_dlt_source_system_unsupported_format_end_to_end(self, mock_filesystem):
        """Test dlt_source_system with unsupported format."""
        uri = "file:///path/to/directory"
        table_name = "test.txt"
        mock_filesystem.return_value = MagicMock()
        with self.assertRaises(ValueError) as context:
            self.local_source.dlt_source_system(uri, table_name)
        self.assertIn("Unsupported file format for table: test.txt", str(context.exception))

    @patch('ferry.src.sources.local_file_source.read_csv')
    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_dlt_source_system_with_kwargs(self, mock_filesystem, mock_read_csv):
        """Test dlt_source_system with unused kwargs."""
        uri = "file:///path/to/directory"
        table_name = "test.csv"
        mock_resource = MagicMock()
        mock_filesystem.return_value = mock_resource
        mock_read_csv.return_value = MagicMock()
        mock_resource.__or__.return_value = "csv_resource"
        result = self.local_source.dlt_source_system(uri, table_name, foo="bar")
        mock_filesystem.assert_called_once_with(bucket_url="/path/to/directory", file_glob="test.csv*")
        self.assertEqual(result, "csv_resource")

    @patch('ferry.src.sources.local_file_source.filesystem')
    def test_dlt_source_system_permissions_error(self, mock_filesystem):
        """Test dlt_source_system with insufficient permissions."""
        uri = "file:///path/to/directory"
        table_name = "test.csv"
        mock_filesystem.side_effect = Exception("Permission Denied")
        with self.assertRaises(Exception) as context:
            self.local_source.dlt_source_system(uri, table_name)
        self.assertIn("Permission Denied", str(context.exception))

if __name__ == "__main__":
    unittest.main()