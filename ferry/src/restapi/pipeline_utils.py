# pipeline_utils.py
import logging
from typing import List

import dlt
from dlt.sources.sql_database import sql_database
import yaml
from dlt.sources.credentials import ConnectionStringCredentials

from models import LoadDataRequest, LoadDataResponse

logger = logging.getLogger(__name__)

def load_config(config_path: str) -> dict:
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise ValueError(f"Configuration file not found at {config_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")
    

def create_connection_string(config: dict, source_or_dest: str, db_type: str) -> str:
    db_config = config.get(source_or_dest, {}).get(db_type)
    if not db_config:
        raise ValueError(f"Missing configuration for {source_or_dest}.{db_type}")
    
    if db_type == "postgres":
        return f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    elif db_type == "clickhouse":
        return f"clickhouse://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
def create_credentials(config: dict, source_or_dest: str, db_type: str) -> ConnectionStringCredentials:
    connection_string = create_connection_string(config, source_or_dest, db_type)
    return ConnectionStringCredentials(connection_string)

def create_pipeline(pipeline_name: str, config: dict, full_refresh: bool = False) -> dlt.Pipeline:
    destination_config = config.get("destination", {}).get("clickhouse")
    if not destination_config:
        raise ValueError("Missing 'destination.clickhouse' configuration")

    credentials = create_credentials(config, "destination", "clickhouse")
    pipeline_settings = {
        "destination": dlt.destinations.clickhouse(credentials), # type: ignore
        "dataset_name": destination_config["dataset_name"],
    }
    return dlt.pipeline(pipeline_name=pipeline_name, full_refresh=full_refresh, **pipeline_settings)


@dlt.source
def postgres_source(config: dict, table_names: List[str]):
    postgres_config = config.get("source", {}).get("postgres")
    if not postgres_config:
        raise ValueError("Missing 'source.postgres' configuration")
    credentials = create_credentials(config, "source", "postgres")
    source = sql_database(credentials)

    for table_name in table_names:

        table_details = config["tables"][table_name]
        incremental_key = table_details.get("incremental_key")

        @source.with_resources(table_name) # type: ignore
        def table_resource(table_name=table_name):
            if incremental_key:
                return source.table(table_name).apply_hints(
                    incremental=dlt.sources.incremental(incremental_key)
                )
            else:
                return source.table(table_name)
            
    return source

async def load_data_endpoint(request: LoadDataRequest, config_path: str) -> LoadDataResponse:
    try:
        config = load_config(config_path)
        pipeline = create_pipeline("sql_db_to_clickhouse", config, request.full_refresh)

        source = postgres_source(config, request.table_names)

        pipeline.run(source, write_disposition=request.write_disposition)

        return LoadDataResponse(
            status="success",
            message="Data loaded successfully",
            pipeline_name=pipeline.pipeline_name,
            tables_processed=request.table_names
        )
    except ValueError as e:
        logger.error(f"Configuration Error: {e}")
        raise
    except Exception as e:
        logger.exception(f"Error in load_data_endpoint: {e}")
        raise