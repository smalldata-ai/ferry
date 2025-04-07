import pymongo
import dlt
import logging
from typing import List, Iterator, Any

from ferry.src.data_models.ingest_model import ResourceConfig
from ferry.src.sources.mongodb.mongodb_collection import mongodb_collection

logger = logging.getLogger(__name__)


class MongoDbSource:
    def dlt_source_system(self, uri: str, collections: List[ResourceConfig], identity: str):
        """
        Creates a DLT source from MongoDB collections based on the provided configuration.
        """
        try:
            client = pymongo.MongoClient(uri)
            database = client.get_database()
            if not database or not database.name:
                raise ValueError("MongoDB URI must specify a valid database.")
        except Exception as e:
            logger.error(f"Invalid MongoDB URI: {uri} | Error: {e}")
            raise ValueError("Invalid MongoDB URI.") from e

        logger.info(f"Connected to MongoDB: {uri}, Database: {database.name}")
        resources = []

        for config in collections:
            collection_name = config.source_table_name
            logger.info(f"Processing collection: {collection_name}")

            if not collection_name:
                raise ValueError("Source collection name must be provided in ResourceConfig.")

            incremental = None
            if config.incremental_config:
                inc_cfg = config.incremental_config
                incremental = dlt.sources.incremental(
                    cursor_path=inc_cfg.incremental_key,
                    initial_value=inc_cfg.start_position or None,
                    lag=inc_cfg.lag_window or 0,
                )
                logger.info(f"Incremental enabled for {collection_name} on '{inc_cfg.incremental_key}'")

            
            try:
                collection_data = mongodb_collection(
                    connection_url=uri,
                    database=database.name,
                    collection=collection_name,
                )
            except Exception as e:
                logger.error(f"Failed to initialize MongoDB collection {collection_name}: {e}")
                raise

            def make_resource_function(data: Iterator[Any]):
                @dlt.resource(
                    name=collection_name,
                    write_disposition=config.write_disposition_config.strategy,
                    primary_key=config.primary_key or "_id",
                    incremental=incremental,
                    merge_key=getattr(config, "merge_key", None),
                    columns=getattr(config, "columns", None),
                )
                def resource():
                    try:
                        yield from data
                    except Exception as e:
                        logger.error(f"Error fetching data from MongoDB collection {collection_name}: {e}")
                        raise

                return resource

            resources.append(make_resource_function(collection_data))

        @dlt.source
        def mongodb_source():
            return resources

        return mongodb_source
