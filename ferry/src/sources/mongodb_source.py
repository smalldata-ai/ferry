import dlt
import logging
import pymongo
from typing import List
from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig
from ferry.src.sources.mongodb import mongodb_collection

logger = logging.getLogger(__name__)

class MongoDbSource(SourceBase):
    def __init__(self) -> None:
        super().__init__()

    def dlt_source_system(self, uri: str, collections: List[ResourceConfig], identity: str):
        """Fetch multiple collections from MongoDB (including Atlas) and create a DLT source."""

        # Handle MongoDB Atlas & Local URI
        try:
            client = pymongo.MongoClient(uri)
            database = client.get_database()  
            if database.name is None:
                raise ValueError("MongoDB URI is missing a valid database name.")
        except Exception as e:
            logger.error(f"Invalid MongoDB URI: {uri} | Error: {e}")
            raise ValueError("Invalid MongoDB URI.") from e

        logger.info(f"Connecting to MongoDB: {uri}, Database: {database.name}")

        source_resources = []

        for collection_config in collections:
            collection_name = collection_config.source_table_name
            logger.info(f"Processing collection: {collection_name}")

            incremental = None
            incremental_config = collection_config.incremental_config
            try:
                if incremental_config and hasattr(incremental_config, "incremental_key"):
                    incremental = dlt.sources.incremental(
                        cursor_path=incremental_config.incremental_key,
                        initial_value=getattr(incremental_config, "start_position", None),
                        lag=getattr(incremental_config, "lag_window", 0),
                    )
                    logger.info(f"Incremental loading enabled for {collection_name}")
            except Exception as e:
                logger.error(f"Error setting incremental ingestion for {collection_name}: {e}")

            try:
                source = mongodb_collection(
                    connection_url=uri,
                    database=database.name,  
                    collection=collection_name,
                )
            except Exception as e:
                logger.error(f"Failed to initialize MongoDB collection {collection_name}: {e}")
                raise

            @dlt.resource(
                name=collection_name,
                incremental=incremental,
                write_disposition=getattr(collection_config, "write_disposition_config", "merge"),  
                primary_key=getattr(collection_config, "primary_key", "_id"),
                merge_key=getattr(collection_config, "merge_key", None),
                columns=getattr(collection_config, "columns", None),
            )
            def resource_function():
                try:
                    yield from source
                except Exception as e:
                    logger.error(f"Error fetching data from MongoDB collection {collection_name}: {e}")
                    raise

            source_resources.append(resource_function)

        
        return dlt.source(
            schema=dlt.Schema(identity),
            section="mongodb_source",
            resources=source_resources
        )
