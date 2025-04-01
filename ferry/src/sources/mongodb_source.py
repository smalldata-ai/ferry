import dlt
import logging
import pymongo
from typing import List
from urllib.parse import urlparse
from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig
from ferry.src.sources.mongodb import mongodb_collection

logger = logging.getLogger(__name__)

class MongoDbSource(SourceBase):
    def __init__(self) -> None:
        super().__init__()

    def dlt_source_system(self, uri: str, collections: List[ResourceConfig], identity: str):
        """Fetch multiple collections from MongoDB (including Atlas) and create a DLT source."""

        #  Handle MongoDB Atlas & Local URI
        try:
            client = pymongo.MongoClient(uri)
            database = client.get_database().name  
            if not database:
                raise ValueError("MongoDB URI is missing a valid database name.")
        except Exception as e:
            logger.error(f"Invalid MongoDB URI: {uri} | Error: {e}")
            raise ValueError("Invalid MongoDB URI.") from e

        logger.info(f"Connecting to MongoDB: {uri}, Database: {database}")

        source_resources = []

        for collection_config in collections:
            collection_name = collection_config.source_table_name
            logger.info(f"Processing collection: {collection_name}")

            incremental = None
            incremental_config = collection_config.incremental_config or {}

            # Incremental Loading Per Collection
            if incremental_config.get("incremental_key"):
                try:
                    incremental = dlt.sources.incremental(
                        cursor_path=incremental_config.get("incremental_key"),
                        initial_value=incremental_config.get("start_position"),
                        range_start=incremental_config.get("range_start"),
                        end_value=incremental_config.get("end_position"),
                        range_end=incremental_config.get("range_end"),
                        lag=incremental_config.get("lag_window", 0),
                    )
                    logger.info(f" Incremental loading enabled for {collection_name}")
                except Exception as e:
                    logger.error(f" Error setting incremental ingestion for {collection_name}: {e}")

            # Initialize MongoDB Collection Source
            try:
                source = mongodb_collection(
                    connection_url=uri,
                    database=database,
                    collection=collection_name,
                )
            except Exception as e:
                logger.error(f"Failed to initialize MongoDB collection {collection_name}: {e}")
                raise

            @dlt.resource(
                name=collection_name,
                incremental=incremental,
                write_disposition=getattr(collection_config, "write_disposition_config", "merge"),  
                primary_key=collection_config.primary_key or "_id",
                merge_key=getattr(collection_config, "merge_key", None),
                columns=getattr(collection_config, "columns", None),
            )
            def resource_function():
                try:
                    yield from source
                except Exception as e:
                    logger.error(f" Error fetching data from MongoDB collection {collection_name}: {e}")
                    raise

            source_resources.append(resource_function)

        #  Return DLT Source with Multiple Collections
        return dlt.DltSource(
            schema=dlt.Schema(identity),
            section="mongodb_source",
            resources=source_resources
        )
