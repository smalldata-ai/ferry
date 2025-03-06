import unittest
from unittest.mock import patch, MagicMock
from ferry.src.sources.postgres_source import PostgresSource
from ferry.src.uri_validator import DatabaseURIValidator
from ferry.src.sources.source_base import SourceBase

class TestPostgresSource(unittest.TestCase):

    @patch.object(DatabaseURIValidator, 'validate_uri')  # Mock URI validation
    @patch.object(SourceBase, 'create_credentials', return_value={
        "drivername": "postgresql",
        "username": "user",
        "password": "password",
        "host": "localhost",
        "port": 5432,
        "database": "dbname"
    })  # Mock create_credentials at the parent class
    @patch('ferry.src.sources.postgres_source.sql_database')  # Mock sql_database
    def test_dlt_source_system(self, mock_sql_database, mock_create_credentials, mock_validate_uri):
        """Test dlt_source_system ensuring no real DB calls are made."""
        
        # Mock sql_database return
        mock_source = MagicMock()
        mock_sql_database.return_value = mock_source
        
        # Test input
        uri = "postgresql://user:password@localhost:5432/dbname"
        table_name = "test_table"
        
        # Create PostgresSource instance
        source = PostgresSource(uri)
        
        # Call the method under test
        result = source.dlt_source_system(uri, table_name)

        # Assertions
        mock_create_credentials.assert_called_once_with(uri)  #  Ensure create_credentials is called
        mock_sql_database.assert_called_once_with(mock_create_credentials.return_value)
        mock_source.with_resources.assert_called_once_with(table_name)
        self.assertEqual(result, mock_source.with_resources.return_value)

if __name__ == '__main__':
    unittest.main()
