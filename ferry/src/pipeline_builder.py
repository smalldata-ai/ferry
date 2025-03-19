from typing import Optional, List
import dlt
import logging
from dlt.common.pipeline import LoadInfo
from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory
from dlt.common.runtime.collector import LogCollector
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class PipelineBuider:

    def __init__(self, model: IngestModel):
        self.model = model
        logger.debug(f"Initializing PipelineBuilder with model: {model}")
        self.destination = DestinationFactory.get(self.model.destination_uri)
        self.source = SourceFactory.get(self.model.source_uri)
        self.destination_table_name = None
        self.source_resource = None
        self.pipeline = None
        self.filtered_columns = self._build_columns()  # Precompute filtered columns


    def build(self):
        try:
            destination = self.destination.dlt_target_system(self.model.destination_uri)
            self.destination_table_name = getattr(self.model.destination_meta, 'table_name', self.model.source_table_name)
            self.pipeline = dlt.pipeline(
                pipeline_name=self.model.identity, 
                dataset_name=getattr(self.model.destination_meta, 'dataset_name', self.destination.default_schema_name()),
                destination=destination, progress=LogCollector(),
                dev_mode=False
            )
            self._build_source_resource()
            logger.info(f"Pipeline '{self.model.identity}' built successfully.")
            return self
        except Exception as e:
            logger.exception(f"Failed to create pipeline: {e}")
            raise RuntimeError(f"Pipeline creation failed: {str(e)}")

    def _build_source_resource(self):
        self.source_resource = self.source.dlt_source_system(
            uri=self.model.source_uri, 
            table_name=self.model.source_table_name, 
            incremental_config=self._build_incremental_config(),
            write_disposition=self.model.build_wd_config(),
            primary_key=self._build_primary_key(),
            merge_key=self._build_merge_key(),
        )

        # Apply column filtering if needed
        if self.filtered_columns:
            self.source_resource.add_map(self.filtered_columns)  # Apply filtering function

        return self



    def run(self):
        if not self.pipeline:
            raise RuntimeError("Pipeline has not been built. Call build() first.")

        try:
            run_info: LoadInfo = self.pipeline.run(
                data=self.source_resource, 
                table_name=self.destination_table_name
            )
            
            logger.info(f"Pipeline run completed successfully: {run_info.metrics}")
            logger.debug(f"Load packages: {run_info.load_packages}")
            logger.debug(f"Writer metrics: {run_info.writer_metrics_asdict}")
            
        except Exception as e:
            logger.exception(f"Unexpected error in full load: {e}")
            raise RuntimeError(f"Pipeline run failed: {str(e)}")

    def get_name(self) -> str:
        return self.pipeline.pipeline_name if self.pipeline else "Pipeline not built"

    def _build_incremental_config(self) -> Optional[dict]:
        return self.model.incremental_config.build_config() if getattr(self.model, 'incremental_config', None) else None
        
    def _build_primary_key(self) -> Optional[dict]:
        return self.model.merge_config.build_pk_config() if getattr(self.model, 'merge_config', None) else None
    
    def _build_merge_key(self) -> Optional[dict]:
        return self.model.merge_config.build_merge_key() if getattr(self.model, 'merge_config', None) else None
    
    def _build_columns(self) -> Optional[List[str]]:
        """Filters columns to exclude specified ones from both source and destination."""
        exclude_columns = getattr(self.model.destination_meta, 'exclude_columns', [])

        # If no exclusions, return None (meaning use all columns)
        if not exclude_columns:
            return None

        # Use only columns that are not in the exclude list
        def remove_columns(doc: Dict) -> Dict:
            return {key: value for key, value in doc.items() if key not in exclude_columns}

        logger.info(f"Applying column filtering, excluding: {exclude_columns}")
        return remove_columns  # Returns the function, not a list



    def __repr__(self):
        return (f"DataPipeline(source_uri={self.model.source_uri}, "
                f"destination_uri={self.model.destination_uri}, "
                f"source_table={self.model.source_table_name}, "
                f"destination_table={self.destination_table_name}, "
                f"excluded_columns={getattr(self.model.destination_meta, 'exclude_columns', []) if self.model.destination_meta else []})")
