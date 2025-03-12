import unittest
from ferry.src.source_factory import SourceFactory
from ferry.src.sources.s3_source import S3Source
from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.sql_db_source import SqlDbSource

class TestSourceFactory(unittest.TestCase):
    
    def test_get_postgres_source(self):
        """Test that a PostgreSQL URI returns a PostgresSource instance."""
        uri = "postgresql://user:password@localhost:5432/dbname"
        source = SourceFactory.get(uri)
        self.assertIsInstance(source, SqlDbSource)
    
    def test_get_duckdb_source(self):
        """Test that a DuckDB URI returns a SqlDbSource instance."""
        uri = "duckdb:///path/to/database.db"
        source = SourceFactory.get(uri)
        self.assertIsInstance(source, SqlDbSource)
    
    def test_get_s3_source(self):
        """Test that an S3 URI returns an S3Source instance."""
        uri = "s3://bucket-name?file_key=data.csv"
        source = SourceFactory.get(uri)
        self.assertIsInstance(source, S3Source)
    
    def test_invalid_source(self):
        """Test that an invalid URI scheme raises an InvalidSourceException."""
        uri = "invalidscheme://user:password@localhost:1234/dbname"
        with self.assertRaises(InvalidSourceException) as context:
            SourceFactory.get(uri)
        self.assertEqual(str(context.exception), "Invalid source URI scheme: invalidscheme")


if __name__ == '__main__':
    unittest.main()
