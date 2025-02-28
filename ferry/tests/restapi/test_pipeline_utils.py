import unittest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
from ferry.src.restapi.models import LoadDataRequest
from ferry.src.restapi.pipeline_utils import create_pipeline, load_data_endpoint
from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory
import dlt

class TestLoadData(unittest.TestCase):
    
    @patch.object(DestinationFactory, 'get')
    @patch('dlt.pipeline')
    def test_create_pipeline(self, mock_dlt_pipeline, mock_get_destination):
        """Test creating a DLT pipeline with a valid destination URI."""
        mock_destination = MagicMock()
        mock_get_destination.return_value.dlt_target_system.return_value = mock_destination
        mock_pipeline = MagicMock()
        mock_dlt_pipeline.return_value = mock_pipeline

        pipeline = create_pipeline("test_pipeline", "postgresql://user:pass@localhost/db", "test_dataset")
        
        mock_get_destination.assert_called_once_with("postgresql://user:pass@localhost/db")
        mock_dlt_pipeline.assert_called_once_with(pipeline_name="test_pipeline", destination=mock_destination, dataset_name="test_dataset")
        self.assertEqual(pipeline, mock_pipeline)
    
    @patch.object(SourceFactory, 'get')
    @patch('dlt.pipeline')
    async def test_load_data_endpoint(self, mock_dlt_pipeline, mock_get_source):
        """Test the load_data_endpoint function."""
        mock_pipeline = MagicMock()
        mock_dlt_pipeline.return_value = mock_pipeline
        mock_source = MagicMock()
        mock_get_source.return_value.dlt_source_system.return_value = mock_source

        request = LoadDataRequest(
            source_uri="s3://bucket-name?file_key=data.csv",
            destination_uri="postgresql://user:pass@localhost/db",
            dataset_name="test_dataset",
            source_table_name="source_table",
            destination_table_name="destination_table"
        )

        response = await load_data_endpoint(request)

        mock_dlt_pipeline.assert_called_once()
        mock_get_source.assert_called_once_with("s3://bucket-name?file_key=data.csv")
        mock_pipeline.run.assert_called_once_with(mock_source, table_name="destination_table", write_disposition="replace")
        
        self.assertEqual(response.status, "success")
        self.assertEqual(response.message, "Data loaded successfully")
        self.assertEqual(response.pipeline_name, mock_pipeline.pipeline_name)
        self.assertEqual(response.table_processed, "destination_table")

    @patch('dlt.pipeline', side_effect=Exception("Pipeline creation failed"))
    def test_create_pipeline_failure(self, mock_dlt_pipeline):
        """Test pipeline creation failure handling."""
        with self.assertRaises(RuntimeError) as context:
            create_pipeline("test_pipeline", "invalid_scheme://localhost", "test_dataset")

        self.assertIn("Pipeline creation failed:", str(context.exception))
        self.assertIn("Invalid destination URI scheme:", str(context.exception))

    @patch.object(SourceFactory, 'get', side_effect=Exception("Invalid source URI"))
    async def test_load_data_endpoint_failure(self, mock_get_source):
        """Test failure scenario in load_data_endpoint."""
        request = LoadDataRequest(
            source_uri="invalid_uri",
            destination_uri="postgresql://user:pass@localhost/db",
            dataset_name="test_dataset",
            source_table_name="source_table",
            destination_table_name="destination_table"
        )

        with self.assertRaises(HTTPException) as context:
            await load_data_endpoint(request)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(context.exception.detail, "An internal server error occurred")

if __name__ == '__main__':
    unittest.main()