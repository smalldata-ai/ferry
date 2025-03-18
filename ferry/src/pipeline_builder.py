from typing import Optional
import dlt
import logging
from dlt.common.pipeline import LoadInfo
from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory
from dlt.common.runtime.collector import LogCollector

logger = logging.getLogger(__name__)
class PipelineBuider:

    def __init__(self, model: IngestModel):
        self.model = model
        self.destination = DestinationFactory.get(self.model.destination_uri)
        self.source = SourceFactory.get(self.model.source_uri)
        self.destination_table_name = None
        self.source_resource = None

    def build(self):
        try:
            destination = self.destination.dlt_target_system(self.model.destination_uri)
            self.destination_table_name = self.model.get_destination_table_name()
            self.pipeline = dlt.pipeline(
                pipeline_name=self.model.identity, 
                dataset_name= self.model.get_dataset_name(self.destination.default_schema_name()),
                destination=destination, progress=LogCollector(),)            
            return self
        except Exception as e:
            logger.exception(f"Failed to create pipeline: {e}")
            raise RuntimeError(f"Pipeline creation failed: {str(e)}")

    def run(self):
        try:
            run_info: LoadInfo = self.pipeline.run(
                    data=self._build_source_resource(), 
                    table_name=self.destination_table_name,)
            logger.info(run_info.metrics)
            logger.info(run_info.load_packages)
            logger.info(run_info.writer_metrics_asdict)
            
        except Exception as e:
            logger.exception(f"Unexpected error in full load: {e}")
            raise e
        
    def _build_source_resource(self):
        return self.source.dlt_source_system(
                uri=self.model.source_uri, 
                table_name=self.model.source_table_name, 
                incremental_config=self._build_incremental_config(),
                write_disposition=self.model.build_wd_config(),
                primary_key=self._build_primary_key(),
                merge_key=self._build_merge_key(),
                columns=self._build_columns()
        )
        
    def get_name(self) -> str:
        return self.pipeline.pipeline_name

    def _build_incremental_config(self) -> Optional[dict]:
        return self.model.incremental_config.build_config() if self.model.incremental_config else None
        
    def _build_primary_key(self) -> Optional[dict]:
        return self.model.merge_config.build_pk_config() if self.model.merge_config else None
    
    def _build_merge_key(self)-> Optional[dict]:
        return self.model.merge_config.build_merge_key() if self.model.merge_config else None
    
    def _build_columns(self)-> Optional[dict]:
        return self.model.merge_config.build_columns() if self.model.merge_config else None
  
    def __repr__(self):
        return (f"DataPipeline(source_uri={self.model.source_uri}, "
            f"destination_uri={self.model.destination_uri}, "
            f"source_table={self.model.source_table_name}, "
            f"destination_table={self.destination_table_name})")
