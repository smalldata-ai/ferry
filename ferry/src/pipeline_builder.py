from typing import List
import dlt
import logging
import dlt.cli
from dlt.common.pipeline import LoadInfo
from numpy import identity
from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory
from dlt.common.runtime.collector import LogCollector

logger = logging.getLogger(__name__)

class PipelineBuilder:


    @classmethod
    def get_pipeline(cls, name: str) :
        return dlt.pipeline(pipeline_name=name)

    def __init__(self, model: IngestModel):
        self.model = model
        self.destination = DestinationFactory.get(self.model.destination_uri)
        self.source = SourceFactory.get(self.model.source_uri)
        self.source_resources = []

    def build(self):
        """Builds the pipeline with multiple resources."""
        try:
            destination = self.destination.dlt_target_system(self.model.destination_uri)
            
            self.pipeline = dlt.pipeline(
                pipeline_name=self.model.identity,
                dataset_name=self.model.get_dataset_name(self.destination.default_schema_name()),
                destination=destination,
                progress=LogCollector(),
                export_schema_path="schemas",
            )
            return self
        except Exception as e:
            logger.exception(f"Failed to create pipeline: {e}")
            raise RuntimeError(f"Pipeline creation failed: {str(e)}")

    def run(self):
        """Runs the pipeline with multiple resources."""
        try:
            run_info: LoadInfo = self.pipeline.run(
                data=self._build_source_resources(),
            )
            logger.info(run_info.metrics)
            logger.info(run_info.load_packages)
            logger.info(run_info.writer_metrics_asdict)
            # meta_pipeline = dlt.pipeline(pipeline_name="metadata", 
            #              destination= DestinationFactory.get("duckdb:////Users/nikhil/Code/my_database.duckdb").dlt_target_system("duckdb:////Users/nikhil/Code/my_database.duckdb")
            #              )
            # meta_pipeline.run(data=[self.pipeline.last_trace], table_name="trace")
        except Exception as e:
            logger.exception(f"Unexpected error in full load: {e}")
            raise e

    def _build_source_resources(self):
        """Builds a list of dlt resources"""
        self.source_resources = self.source.dlt_source_system(
            identity=self.model.identity,
            uri=self.model.source_uri,
            resources=self.model.resources,
        )
        return self.source_resources

    def get_name(self) -> str:
        return self.pipeline.pipeline_name

    def __repr__(self):
        return (f"DataPipeline(source_uri={self.model.source_uri}, "
                f"destination_uri={self.model.destination_uri}, "
                f"source_tables={[r.source_table_name for r in self.model.resources]}, ")