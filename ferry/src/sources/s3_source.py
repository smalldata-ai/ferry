import dlt
import urllib.parse
import hashlib
import logging
from typing import List
from dlt.common.configuration.specs import AwsCredentials
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from dlt.extract.source import DltSource
from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig

logger = logging.getLogger(__name__)

class S3Source(SourceBase):
    def __init__(self):
        super().__init__()

    def dlt_source_system(self, uri: str, resources: List[ResourceConfig], identity: str) -> DltSource:
        """Fetch multiple tables from S3 and create a DLT source with resources."""
        bucket_name, aws_credentials = self._parse_s3_uri(uri)
        resources_list = []

        for resource_config in resources:
            table_name = resource_config.source_table_name
            rules_dict = resource_config.column_rules if resource_config.column_rules else {}

            exclude_columns = rules_dict.get("exclude_columns", [])
            pseudonymizing_columns = rules_dict.get("pseudonymizing_columns", [])

            logger.info(f"Processing table: {table_name}, Excluding columns: {exclude_columns}, Pseudonymizing columns: {pseudonymizing_columns}")

            incremental_column = None
            if resource_config.incremental_config:
                incremental_config = resource_config.incremental_config.build_config()
                incremental_column = incremental_config.get("incremental_key", None)

            write_disposition = resource_config.build_wd_config()
            primary_key = resource_config.merge_config.build_pk_config() if resource_config.merge_config else []
            merge_key = resource_config.merge_config.build_merge_key() if resource_config.merge_config else []
            columns = resource_config.merge_config.build_columns() if resource_config.merge_config else {}

            # Create file resource with incremental loading based on modification date
            file_resource = filesystem(f"s3://{bucket_name}", aws_credentials, f"{table_name}*")
            file_resource.apply_hints(incremental=dlt.sources.incremental("modification_date"))

            # Select reader based on file extension
            lower_table_name = table_name.lower()
            if lower_table_name.endswith(".csv"):
                reader = read_csv
            elif lower_table_name.endswith(".jsonl"):
                reader = read_jsonl
            elif lower_table_name.endswith(".parquet"):
                reader = read_parquet
            else:
                raise ValueError(f"Unsupported file format for table: {table_name}")

            @dlt.resource(
                name=resource_config.get_destination_table_name(),
                incremental=dlt.sources.incremental(incremental_column) if incremental_column else None,
                write_disposition=write_disposition,
                primary_key=primary_key,
                merge_key=merge_key,
                columns=columns,
            )
            def resource_function():
                for row in (file_resource | reader()):
                    if not isinstance(row, dict):
                        logger.warning(f"Skipping non-dictionary row: {row}")
                        continue
                    
                    # Remove excluded columns
                    filtered_row = {k: v for k, v in row.items() if k not in exclude_columns}

                    # Apply pseudonymization
                    self._pseudonymize_columns(filtered_row, pseudonymizing_columns)

                    logger.debug(f"Processed row: {filtered_row}")
                    yield filtered_row

            resources_list.append(resource_function())

        return DltSource(
            schema=dlt.Schema(identity),
            section="s3_source",
            resources=resources_list
        )

    def _parse_s3_uri(self, uri: str):
        """Extracts bucket name and AWS credentials from the URI."""
        parsed_uri = urllib.parse.urlparse(uri)
        bucket_name = parsed_uri.hostname
        query_params = urllib.parse.parse_qs(parsed_uri.query)

        aws_credentials = AwsCredentials(
            aws_access_key_id=query_params.get("access_key_id", [None])[0],
            aws_secret_access_key=query_params.get("access_key_secret", [None])[0],
            region_name=query_params.get("region", [None])[0]
        )
        return bucket_name, aws_credentials

    def _pseudonymize_columns(self, row, pseudonymizing_columns):
        """Pseudonymizes specified columns using SHA-256 hashing."""
        salt = "WI@N57%zZrmk#88c"
        for col in pseudonymizing_columns:
            if col in row and row[col] is not None:
                sh = hashlib.sha256()
                sh.update((str(row[col]) + salt).encode())
                row[col] = sh.hexdigest()
