import dlt
import logging
from dlt.common.pipeline import LoadInfo
from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory

logger = logging.getLogger(__name__)

class Pipeline:

    def __init__(self, ingest_model: IngestModel):
        self.model = ingest_model
        self.source_uri = ingest_model.source_uri
        self.destination_uri = ingest_model.destination_uri
        self.source_table = ingest_model.source_table_name
        self.destination = DestinationFactory.get(self.destination_uri)
        self.source = SourceFactory.get(self.source_uri)
        self.destination_table_name = None

    def build(self):
        try:
            destination = self.destination.dlt_target_system(self.destination_uri)
            self.pipeline = dlt.pipeline(pipeline_name=self._build_pipeline_name(), destination=destination)
            return self
        except Exception as e:
            logger.exception(f"Failed to create pipeline: {e}")
            raise RuntimeError(f"Pipeline creation failed: {str(e)}")

    def add_destination_meta(self, destination_meta):
        if self.model.destination_meta:
            self.pipeline.dataset_name = getattr(destination_meta, 'dataset_name', None)
            self.destination_table_name = getattr(destination_meta, 'table_name', None)

    def add_source_resource(self):
        source = self.source.dlt_source_system(self.source_uri, self.source_table)
    
    def run(self):
        try:
            source = self.source.dlt_source_system(self.source_uri, self.source_table)
            run_info: LoadInfo = self.pipeline.run(source, table_name=self.destination_table_name, write_disposition="replace")
            logger.info(run_info.metrics)
            logger.info(run_info.load_packages)
        except Exception as e:
            logger.exception(f"Unexpected error in full load: {e}")
            raise e
        
    def build_merge_params(self):
        return None
        #   if not request.merge_incremental_load_config:
        #     raise ValueError("merge_incremental_config is required for MERGE write disposition")
        
        # merge_strategy = request.merge_incremental_load_config.merge_strategy
        # if merge_strategy == MergeStrategy.DELETE_INSERT:
        #     source = source.dlt_merge_resource_system(
        #         request.source_uri,
        #         request.source_table_name,
        #         merge_strategy,
        #         request.merge_incremental_load_config.delete_insert_config
        #     )
        # elif merge_strategy == MergeStrategy.SCD2:
        #     source = source.dlt_merge_resource_system(
        #         request.source_uri,
        #         request.source_table_name,
        #         merge_strategy,
        #         request.merge_incremental_load_config.scd2_config
        #     )
        # elif merge_strategy == MergeStrategy.UPSERT:
        #     source = source.dlt_merge_resource_system(
        #         request.source_uri,
        #         request.source_table_name,
        #         merge_strategy,
        #         request.merge_incremental_load_config.upsert_config
        #     )
        # else:
        #     raise ValueError(f"Unsupported merge strategy: {merge_strategy}")
    
  
    def _build_pipeline_name(self):
        source_tag = self.source.dlt_source_name(self.source_uri, self.source_table)
        destination_tag = self.destination.dlt_destination_name(self.destination_uri, self.destination_table)
        f"{source_tag}:{destination_tag}"
     
    def __repr__(self):
        return (f"DataPipeline(source_uri={self.source_uri}, "
            f"destination_uri={self.destination_uri}, "
            f"source_table={self.source_table}, "
            f"destination_table={self.destination_table})")
