import dlt
import logging
import hashlib
from typing import List
from dlt.extract.source import DltSource
from ferry.src.data_models.ingest_model import ResourceConfig
from ferry.src.sources.source_base import SourceBase
from ferry.src.source_factory import SourceFactory
from ferry.src.destination_factory import DestinationFactory


logger = logging.getLogger(__name__)

class PipelineBuilder:
    
    def __init__(self, model):
        self.model = model
        self.destination = DestinationFactory.get(self.model.destination_uri)
        self.source = SourceFactory.get(self.model.source_uri)
        self.pipeline = None
    
    def build(self):
        """Builds the pipeline with multiple resources."""
        try:
            destination = self.destination.dlt_target_system(self.model.destination_uri)
            self.pipeline = dlt.pipeline(
                pipeline_name=self.model.identity,
                dataset_name=self.model.get_dataset_name(self.destination.default_schema_name()),
                destination=destination,
                progress=dlt.common.runtime.collector.LogCollector(),
                export_schema_path="schemas",
            )
            return self
        except Exception as e:
            logger.exception(f"Failed to create pipeline: {e}")
            raise RuntimeError(f"Pipeline creation failed: {str(e)}")
    
    def run(self):
        """Runs the pipeline with multiple resources."""
        try:
            source_resources = []
            
            for resource_config in self.model.resources:
                source_resource = self.source.dlt_source_system(
                    uri=self.model.source_uri,
                    resources=[resource_config],  
                    identity=self.model.identity
                )
                source_resources.append(source_resource)

            run_info = self.pipeline.run(data=source_resources)
            logger.info(run_info.metrics)
            logger.info(run_info.load_packages)
            logger.info(run_info.writer_metrics_asdict)

        except Exception as e:
            logger.exception(f"Unexpected error in full load: {e}")
            raise e
        
    def get_name(self) -> str:
        return self.pipeline.pipeline_name
    
    def __repr__(self):
        return (f"DataPipeline(source_uri={self.model.source_uri}, "
                f"destination_uri={self.model.destination_uri}, "
                f"source_tables={[r.source_table_name for r in self.model.resources]}, "
                f"destination_tables={[r.get_destination_table_name() for r in self.model.resources]})")
