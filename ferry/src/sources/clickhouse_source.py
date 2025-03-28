from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig
import pandas as pd
import dlt
import logging
import clickhouse_connect

logger = logging.getLogger(__name__)

class ClickHouseSource(SourceBase):
    
    def __init__(self):
        super().__init__()
        self.client = clickhouse_connect.get_client(
            host="localhost", port=8123, username="admin", password="12345"
        )

    def dlt_source_system(self, resources):
        """Fetch data from ClickHouse and prepare dlt Resources"""
        source_resources = []

        try:
            for resource_config in resources:
                query = f"SELECT * FROM {resource_config.source_table_name}"
                logger.info(f"Running query: {query}")

                # Execute query using clickhouse-connect
                result = self.client.query(query)

                # Convert result to Pandas DataFrame
                df = pd.DataFrame(result.result_rows, columns=result.column_names)

                # Convert DataFrame to dlt Resource
                source_resource = dlt.Resource.from_data(
                    df.to_dict(orient='records'), 
                    name=resource_config.source_table_name
                )
                source_resources.append(source_resource)

        except Exception as e:
            logger.error(f"Error fetching data from ClickHouse: {e}")
            raise e

        return source_resources  

    def get_tables(self):
        """List tables available in ClickHouse"""
        try:
            query = "SHOW TABLES"
            result = self.client.query(query)
            return [row[0] for row in result.result_rows]  # Extract table names
        except Exception as e:
            logger.error(f"Error listing ClickHouse tables: {e}")
            return []

    def validate_connection(self) -> bool:
        """Check if the connection to ClickHouse is valid"""
        try:
            self.client.query("SELECT 1")  # Test query
            return True
        except Exception as e:
            logger.error(f"ClickHouse connection error: {e}")
            return False



# from ferry.src.sources.source_base import SourceBase
# from ferry.src.data_models.ingest_model import ResourceConfig
# from sqlalchemy import create_engine
# import pandas as pd
# import dlt
# import logging

# logger = logging.getLogger(__name__)

# class ClickHouseSource(SourceBase):
    
#     def __init__(self):
#         super().__init__()

#     def dlt_source_system(self, uri: str, resources: list[ResourceConfig], identity: str):
#         """Fetch data from ClickHouse and return as a list of DLT resources"""
#         logger.info(f"Connecting to ClickHouse at {uri}")

#         engine = create_engine(uri)  
#         source_resources = []

#         try:
#             with engine.connect() as connection:
#                 for resource_config in resources:
#                     query = f"SELECT * FROM {resource_config.source_table_name}"
#                     logger.info(f"Running query: {query}")

#                     result = pd.read_sql(query, connection)

                    
#                     source_resource = dlt.Resource.from_data(
#                         result.to_dict(orient='records'), 
#                         name=resource_config.source_table_name
#                     )
#                     source_resources.append(source_resource)

#         except Exception as e:
#             logger.error(f"Error fetching data from ClickHouse: {e}")
#             raise e
#         finally:
#             engine.dispose()  

#         return source_resources  

#     def get_tables(self, uri: str):
#         """List tables available in ClickHouse"""
#         engine = create_engine(uri)
#         try:
#             with engine.connect() as connection:
#                 query = "SHOW TABLES"
#                 result = pd.read_sql(query, connection)
#                 return result['name'].tolist()
#         except Exception as e:
#             logger.error(f"Error listing ClickHouse tables: {e}")
#             return []
#         finally:
#             engine.dispose()  

#     def validate_connection(self, uri: str) -> bool:
#         """Check if the connection to ClickHouse is valid"""
#         try:
#             engine = create_engine(uri)
#             with engine.connect() as connection:
#                 connection.execute("SELECT 1")
#             return True
#         except Exception as e:
#             logger.error(f"ClickHouse connection error: {e}")
#             return False
#         finally:
#             engine.dispose()
