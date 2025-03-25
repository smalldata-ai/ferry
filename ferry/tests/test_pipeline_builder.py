import unittest
from unittest.mock import MagicMock, patch
from ferry.src.pipeline_builder import PipelineBuilder
from ferry.src.data_models.ingest_model import ResourceConfig

class TestPipelineBuilder(unittest.TestCase):
    
    def setUp(self):
        self.mock_model = MagicMock()
        self.mock_model.identity = "test_pipeline"
        self.mock_model.destination_uri = "postgres://user:pass@localhost/db"
        self.mock_model.source_uri = "s3://bucket/path"
        self.mock_model.resources = [MagicMock(spec=ResourceConfig)]
        
        self.mock_destination = MagicMock()
        self.mock_source = MagicMock()
        self.mock_pipeline = MagicMock()

    @patch("ferry.src.destination_factory.DestinationFactory.get")
    @patch("ferry.src.source_factory.SourceFactory.get")
    def test_initialization(self, mock_source_factory, mock_dest_factory):
        mock_source_factory.return_value = self.mock_source
        mock_dest_factory.return_value = self.mock_destination
        
        builder = PipelineBuilder(self.mock_model)
        
        self.assertEqual(builder.model, self.mock_model)
        self.assertEqual(builder.destination, self.mock_destination)
        self.assertEqual(builder.source, self.mock_source)
        self.assertIsNone(builder.pipeline)
    
    @patch("dlt.pipeline")
    @patch("ferry.src.destination_factory.DestinationFactory.get")
    def test_build_pipeline(self, mock_dest_factory, mock_dlt_pipeline):
        mock_dest_factory.return_value = self.mock_destination
        self.mock_destination.dlt_target_system.return_value = "postgres"
        mock_dlt_pipeline.return_value = self.mock_pipeline
        
        builder = PipelineBuilder(self.mock_model)
        builder.build()
        
        self.assertIsNotNone(builder.pipeline)
        self.assertEqual(builder.pipeline, self.mock_pipeline)
        mock_dlt_pipeline.assert_called_once()
    
    @patch("dlt.pipeline")
    @patch("ferry.src.destination_factory.DestinationFactory.get")
    def test_build_pipeline_failure(self, mock_dest_factory, mock_dlt_pipeline):
        mock_dest_factory.return_value = self.mock_destination
        mock_dlt_pipeline.side_effect = Exception("Pipeline Error")
        
        builder = PipelineBuilder(self.mock_model)
        
        with self.assertRaises(RuntimeError) as context:
            builder.build()
        self.assertIn("Pipeline creation failed", str(context.exception))
    
    @patch("dlt.pipeline")
    @patch("ferry.src.source_factory.SourceFactory.get")
    @patch("ferry.src.destination_factory.DestinationFactory.get")
    def test_run_pipeline(self, mock_dest_factory, mock_source_factory, mock_dlt_pipeline):
        mock_dest_factory.return_value = self.mock_destination
        mock_source_factory.return_value = self.mock_source
        mock_dlt_pipeline.return_value = self.mock_pipeline
        self.mock_pipeline.run.return_value = MagicMock(metrics={}, load_packages=[], writer_metrics_asdict={})
        
        builder = PipelineBuilder(self.mock_model)
        builder.build()
        builder.run()
        
        self.mock_pipeline.run.assert_called_once()
    
    @patch("dlt.pipeline")
    @patch("ferry.src.source_factory.SourceFactory.get")
    @patch("ferry.src.destination_factory.DestinationFactory.get")
    def test_run_pipeline_failure(self, mock_dest_factory, mock_source_factory, mock_dlt_pipeline):
        mock_dest_factory.return_value = self.mock_destination
        mock_source_factory.return_value = self.mock_source
        mock_dlt_pipeline.return_value = self.mock_pipeline
        self.mock_pipeline.run.side_effect = Exception("Run Error")
        
        builder = PipelineBuilder(self.mock_model)
        builder.build()
        
        with self.assertRaises(Exception) as context:
            builder.run()
        self.assertIn("Run Error", str(context.exception))
    
    @patch("dlt.pipeline")
    @patch("ferry.src.destination_factory.DestinationFactory.get")
    def test_get_name(self, mock_dest_factory, mock_dlt_pipeline):
        mock_dest_factory.return_value = self.mock_destination
        mock_dlt_pipeline.return_value = self.mock_pipeline
        self.mock_pipeline.pipeline_name = "test_pipeline"
        
        builder = PipelineBuilder(self.mock_model)
        builder.build()
        self.assertEqual(builder.get_name(), "test_pipeline")
    
    def test_repr(self):
        self.mock_model.resources[0].source_table_name = "source_table"
        self.mock_model.resources[0].get_destination_table_name.return_value = "destination_table"
        
        builder = PipelineBuilder(self.mock_model)
        repr_string = repr(builder)
        self.assertIn("test_pipeline", repr_string)
        self.assertIn("s3://bucket/path", repr_string)
        self.assertIn("postgres://user:pass@localhost/db", repr_string)
        self.assertIn("source_table", repr_string)
        self.assertIn("destination_table", repr_string)

if __name__ == "__main__":
    unittest.main()
