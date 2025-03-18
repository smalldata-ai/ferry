import unittest
from unittest.mock import patch, MagicMock
import dlt
from ferry.src.destinations.postgres_destination import PostgresDestination
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator
from ferry.src.exceptions import InvalidSourceException


class TestPostgresDestination(unittest.TestCase):

    @patch.object(DatabaseURIValidator, 'validate_uri')
    def test_validate_uri_valid(self, mock_validate_uri):
        """Test that validate_uri calls the centralized validator for a valid URI."""
        
        uri = "postgresql://user:password@localhost:5432/mydb"
        destination = PostgresDestination()

        destination.validate_uri(uri)

        mock_validate_uri.assert_called_once_with(uri)

    @patch.object(DatabaseURIValidator, 'validate_uri', side_effect=ValueError("Invalid URI"))
    def test_validate_uri_invalid(self, mock_validate_uri):
        """Test that an invalid URI raises InvalidSourceException."""
        
        uri = "invalid-uri"
        destination = PostgresDestination()

        with self.assertRaises(InvalidSourceException) as context:
            destination.validate_uri(uri)

        self.assertIn("Invalid URI", str(context.exception))
        mock_validate_uri.assert_called_once_with(uri)

    @patch.object(PostgresDestination, 'validate_uri')  # Mock validate_uri
    @patch('dlt.destinations.postgres')  # Mock DLT's postgres destination
    def test_dlt_target_system(self, mock_dlt_postgres, mock_validate_uri):
        """Test dlt_target_system correctly initializes the DLT PostgreSQL destination."""
        
        uri = "postgresql://user:password@localhost:5432/mydb"
        destination = PostgresDestination()
        
        mock_dlt_postgres.return_value = "dlt_mock_destination"

        result = destination.dlt_target_system(uri, param1="value1")

        mock_validate_uri.assert_called_once_with(uri)
        mock_dlt_postgres.assert_called_once_with(credentials=uri, param1="value1")
        self.assertEqual(result, "dlt_mock_destination")


if __name__ == "__main__":
    unittest.main()
