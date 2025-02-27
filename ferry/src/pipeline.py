import dlt
import logging
from dlt.common.pipeline import LoadInfo
from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory

logger = logging.getLogger(__name__)

class Pipeline:

  def __init__(self, source_uri, destination_uri, source_table, destination_table):
    self.source_uri = source_uri
    self.destination_uri = destination_uri
    self.source_table = source_table
    self.destination_table = destination_table
    self.destination = DestinationFactory.get(self.destination_uri)
    self.source = SourceFactory.get(self.source_uri)

  def build(self):
        try:
            destination = self.destination.dlt_target_system(self.destination_uri)
            self.pipeline = dlt.pipeline(pipeline_name=self._build_pipeline_name(), destination=destination)
            return self
        except Exception as e:
            logger.exception(f"Failed to create pipeline: {e}")
            raise RuntimeError(f"Pipeline creation failed: {str(e)}")
  
  def run(self):
        try:
            source = self.source.dlt_source_system(self.source_uri, self.source_table)
            run_info: LoadInfo = self.pipeline.run(source, table_name=self.destination_table, write_disposition="replace")
            logger.info(run_info.metrics)
            logger.info(run_info.load_packages)
        except Exception as e:
            logger.exception(f"Unexpected error in full load: {e}")
            raise e
    
  
  def _build_pipeline_name(self):
        source_tag = self.source.dlt_source_name(self.source_uri, self.source_table)
        destination_tag = self.destination.dlt_destination_name(self.destination_uri, self.destination_table)
        f"{source_tag}:{destination_tag}"
     
  def __repr__(self):
        return (f"DataPipeline(source_uri={self.source_uri}, "
            f"destination_uri={self.destination_uri}, "
            f"source_table={self.source_table}, "
            f"destination_table={self.destination_table})")
