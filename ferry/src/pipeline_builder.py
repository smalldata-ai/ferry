from typing import List
import dlt
import logging
from dlt.common.pipeline import LoadInfo
from numpy import identity
from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory
from dlt.common.runtime.collector import LogCollector

logger = logging.getLogger(__name__)

class PipelineBuilder:

    def __init__(self, model: IngestModel):
        self.model = model
        self.destination = DestinationFactory.get(self.model.destination_uri)
        self.source = SourceFactory.get(self.model.source_uri)
        self.destination_table_names = []
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
        except Exception as e:
            logger.exception(f"Unexpected error in full load: {e}")
            raise e

    def _build_source_resources(self):
        """Creates dlt resources for both SQL and S3 with column exclusion support."""
        self.source_resources = []

        for resource_config in self.model.resources:
            table_name = resource_config.source_table_name
            exclude_columns = resource_config.exclude_columns or []

            logger.info(f"Processing table: {table_name}, Excluding columns: {exclude_columns}")

            incremental = None
            if resource_config.incremental_config:
                incremental_config = resource_config.incremental_config.build_config()
                incremental = dlt.sources.incremental(
                    cursor_path=incremental_config.get("incremental_key", None),
                    initial_value=incremental_config.get("start_position", None),
                    range_start=incremental_config.get("range_start", None),
                    end_value=incremental_config.get("end_position", None),
                    range_end=incremental_config.get("range_end", None),
                    lag=incremental_config.get("lag_window", 0),
                )

            write_disposition = resource_config.build_wd_config()

            primary_key = (
                resource_config.merge_config.build_pk_config()
                if resource_config.merge_config
                else []
            )

            merge_key = (
                resource_config.merge_config.build_merge_key()
                if resource_config.merge_config
                else []
            )

            columns = (
                resource_config.merge_config.build_columns()
                if resource_config.merge_config
                else {}
            )

            @dlt.resource(
                name=resource_config.get_destination_table_name(),
                incremental=incremental,
                write_disposition=write_disposition,
                primary_key=primary_key,
                merge_key=merge_key,
                columns=columns,
            )
            def resource_function(table_name=table_name, exclude_columns=exclude_columns):
                data = self.source.dlt_source_system(
                    uri=self.model.source_uri,
                    table_name=table_name,
                    resources=[resource_config],  # Passing full config
                )

                for row in data:
                    if not isinstance(row, dict):
                        logger.warning(f"Skipping non-dictionary row: {row}")
                        continue

                    filtered_row = {k: v for k, v in row.items() if k not in exclude_columns}
                    logger.debug(f"Processed row: {filtered_row}")
                    yield filtered_row

            self.source_resources.append(resource_function())

        self.destination_table_names = [
            resource_config.get_destination_table_name() for resource_config in self.model.resources
        ]
        return self.source_resources

    def get_name(self) -> str:
        return self.pipeline.pipeline_name

    def __repr__(self):
        return (f"DataPipeline(source_uri={self.model.source_uri}, "
                f"destination_uri={self.model.destination_uri}, "
                f"source_tables={[r.source_table_name for r in self.model.resources]}, "
                f"destination_tables={self.destination_table_names})")