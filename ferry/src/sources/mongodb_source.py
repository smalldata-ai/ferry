import dlt
from urllib.parse import urlparse, parse_qs
from ferry.src.sources.source_base import SourceBase

try:
    from ferry.src.sources.mongodb import mongodb_collection
except ImportError:
    from ferry.src.sources.mongodb import mongodb_collection


class MongoDbSource(SourceBase):
    def __init__(self) -> None:
        super().__init__()

    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        try:
            parsed_uri = urlparse(uri)
            query_params = parse_qs(parsed_uri.query)
            database = query_params.get("database", [None])[0]
            if not database:
                raise ValueError("MongoDB URI is missing the 'database' parameter.")
        except ValueError as e:
            raise ValueError(f"Invalid MongoDB URI: {e}")

        credentials = self.create_credentials(uri)

        source = mongodb_collection(
            connection_url=credentials.to_native_representation(),
            database=database,
            collection=table_name,
        )

        incremental = None
        if kwargs.get("incremental_config"):
            incremental_config = kwargs.get("incremental_config")
            incremental = dlt.sources.incremental(
                cursor_path=incremental_config.get("incremental_key", None),
                initial_value=incremental_config.get("start_position", None),
                range_start=incremental_config.get("range_start", None),
                end_value=incremental_config.get("end_position", None),
                range_end=incremental_config.get("range_end", None),
                lag=incremental_config.get("lag_window", 0),
            )

        @dlt.resource(
            name=table_name,
            incremental=incremental,
            write_disposition=kwargs.get("write_disposition", ""),
            primary_key=kwargs.get("primary_key", "_id"),
            merge_key=kwargs.get("merge_key", ""),
            columns=kwargs.get("columns", None),
        )
        def resource_function():
            yield from source

        return resource_function
