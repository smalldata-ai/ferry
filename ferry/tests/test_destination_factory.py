import pytest
from ferry.src.destination_factory import DestinationFactory
from ferry.src.destinations.athena_destination import AthenaDestination
from ferry.src.destinations.big_query_destination import BigQueryDestination
from ferry.src.destinations.motherduck_destination import MotherduckDestination
from ferry.src.destinations.mssql_destination import MssqlDestination
from ferry.src.destinations.postgres_destination import PostgresDestination
from ferry.src.destinations.clickhouse_destination import ClickhouseDestination
from ferry.src.destinations.duckdb_destination import DuckDBDestination
from ferry.src.destinations.snowflake_destination import SnowflakeDestination
from ferry.src.destinations.sql_alchemy_destination import SqlAlchemyDestination
from ferry.src.destinations.syanpse_destination import SynapseDestination
from ferry.src.exceptions import InvalidDestinationException

@pytest.mark.parametrize(
    "uri, expected_class",
    [
        ("postgres://user:password@localhost:5432/dbname", PostgresDestination),
        ("postgresql://user:password@localhost:5432/dbname", PostgresDestination),
        ("clickhouse://user:password@localhost:9000/dbname", ClickhouseDestination),
        ("duckdb:////path/to/database.db", DuckDBDestination),
        ("snowflake://user:password/ferrytest/MY_DATASET", SnowflakeDestination),
        ("md://ferrytest?token=token", MotherduckDestination),
        ("synapse://username:password@workspace_name.azuresynapse.net/db_name", SynapseDestination),
        ("sqlite:////path/to/database.db", SqlAlchemyDestination),
        ("mysql://user:password@localhost:3306/dbname", SqlAlchemyDestination),
        ("mssql://user:pass@host/db", MssqlDestination),
        ("athena://bucket_name?", AthenaDestination),
        ("bigquery://project_id?", BigQueryDestination),
        
    ]
)
def test_destination_factory(uri, expected_class):
    """Test that the URI returns the expected destination instance."""
    destination = DestinationFactory.get(uri)
    assert isinstance(destination, expected_class)

def test_invalid_destination():
    """Test that an invalid URI scheme raises an InvalidDestinationException."""
    uri = "invalidscheme://user:password@localhost:1234/dbname"
    with pytest.raises(InvalidDestinationException, match="Invalid destination URI scheme: invalidscheme"):
        DestinationFactory.get(uri)


