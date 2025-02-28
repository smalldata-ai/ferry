import unittest
from ferry.src.destination_factory import DestinationFactory
from ferry.src.destinations.postgres_destination import PostgresDestination
from ferry.src.destinations.clickhouse_destination import ClickhouseDestination
from ferry.src.destinations.duckdb_destination import DuckDBDestination
from ferry.src.exceptions import InvalidDestinationException

class TestDestinationFactory(unittest.TestCase):
    
    def test_get_postgres_destination(self):
        """Test that a PostgreSQL URI returns a PostgresDestination instance."""
        uri = "postgresql://user:password@localhost:5432/dbname"
        destination = DestinationFactory.get(uri)
        self.assertIsInstance(destination, PostgresDestination)
    
    def test_get_clickhouse_destination(self):
        """Test that a ClickHouse URI returns a ClickhouseDestination instance."""
        uri = "clickhouse://user:password@localhost:9000/dbname"
        destination = DestinationFactory.get(uri)
        self.assertIsInstance(destination, ClickhouseDestination)
    
    def test_get_duckdb_destination(self):
        """Test that a DuckDB URI returns a DuckDBDestination instance."""
        uri = "duckdb:///path/to/database.db"
        destination = DestinationFactory.get(uri)
        self.assertIsInstance(destination, DuckDBDestination)
    
    def test_invalid_destination(self):
        """Test that an invalid URI scheme raises an InvalidDestinationException."""
        uri = "invalidscheme://user:password@localhost:1234/dbname"
        with self.assertRaises(InvalidDestinationException) as context:
            DestinationFactory.get(uri)
        self.assertEqual(str(context.exception), "Invalid destination URI scheme: invalidscheme")

if __name__ == '__main__':
    unittest.main()
