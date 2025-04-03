import unittest
from unittest.mock import patch, MagicMock
from typing import List
from ferry.src.sources.local_file_source import LocalFileSource
from ferry.src.data_models.ingest_model import ResourceConfig, IncrementalConfig, WriteDispositionConfig, WriteDispositionType

class TestLocalFileSource(unittest.TestCase):
    def setUp(self):
        """Setup test environment."""
        self.mock_uri = "file:///tmp/test_data"
        
        # Corrected ResourceConfig with all required fields
        self.mock_resources: List[ResourceConfig] = [
            ResourceConfig(
                source_table_name="test_table.csv",
                primary_key="id",  
                destination_table_name="destination_table",
                column_rules=None,
                write_disposition_config=WriteDispositionConfig(type=WriteDispositionType.APPEND),
                incremental_config=IncrementalConfig(incremental_key="updated_at")
            )
        ]
        
        self.local_source = LocalFileSource()

    @patch("urllib.parse.urlparse")
    def test_parse_local_uri(self, mock_urlparse):
        """Test parsing a local filesystem URI."""
        mock_urlparse.return_value.scheme = "file"
        mock_urlparse.return_value.path = "/tmp/test_data"
        result = self.local_source._parse_local_uri(self.mock_uri)
        self.assertEqual(result, "/tmp/test_data")

    def test_parse_local_uri_invalid_scheme(self):
        """Test parsing an unsupported URI scheme."""
        with self.assertRaises(ValueError):
            self.local_source._parse_local_uri("http://example.com/data")

    @patch("dlt.sources.filesystem")
    @patch("pandas.read_csv")
   
    def test_dlt_source_system(self, mock_read_csv, mock_filesystem):
        """Test the dlt_source_system function with multiple resources."""
        mock_filesystem.return_value = MagicMock()
        mock_read_csv.return_value = iter([{"id": 1, "name": "Test", "updated_at": "2024-01-01"}])

        self.mock_resources[0].file_pattern = "test_table*.csv"  

        # ✅ Dynamically add 'columns' attribute before calling the method
        setattr(self.mock_resources[0], "columns", ["id", "name", "updated_at"])

        result = self.local_source.dlt_source_system(self.mock_uri, self.mock_resources, "test_identity")


    def test_get_reader_csv(self):
        """Test getting a CSV reader function."""
        reader = self.local_source._get_reader("test_table.csv")
        self.assertIsNotNone(reader)

    def test_get_reader_jsonl(self):
        """Test getting a JSONL reader function."""
        reader = self.local_source._get_reader("test_table.jsonl")
        self.assertIsNotNone(reader)

    def test_get_reader_parquet(self):
        """Test getting a Parquet reader function."""
        reader = self.local_source._get_reader("test_table.parquet")
        self.assertIsNotNone(reader)

    def test_get_reader_invalid_format(self):
        """Test error handling for an unsupported file format."""
        with self.assertRaises(ValueError):
            self.local_source._get_reader("test_table.txt")

    def test_get_row_incremental(self):
        """Test extracting the incremental key from ResourceConfig."""
        result = self.local_source._get_row_incremental(self.mock_resources[0])
        self.assertIsNotNone(result)

if __name__ == "__main__":
    unittest.main()
