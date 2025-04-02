import unittest
from unittest.mock import patch, MagicMock, mock_open
import json
import time
import hmac
import hashlib
import grpc

# Import the module to be tested
from ferry.src.grpc.grpc_server import (
    validate_request_metadata,
    load_client_secrets,
    FerryServiceServicer,
    serve,
)
from ferry.src.grpc.protos import ferry_pb2


class TestGrpcServer(unittest.TestCase):
    def setUp(self):
        # Reset global variables before each test
        import ferry.src.grpc.grpc_server

        ferry.src.grpc.grpc_server.CLIENT_ID = "test_client_id"
        ferry.src.grpc.grpc_server.CLIENT_SECRET = "test_client_secret"
        ferry.src.grpc.grpc_server.SECURE_MODE = False

    def test_load_client_secrets_success(self):
        # Mock the file open and json load
        mock_secrets = {"client_id": "new_client_id", "client_secret": "new_client_secret"}

        with (
            patch("builtins.open", mock_open(read_data=json.dumps(mock_secrets))),
            patch("os.path.join", return_value="mock_path"),
        ):
            # Call the function
            load_client_secrets()

            # Check the global variables were set correctly
            import ferry.src.grpc.grpc_server

            self.assertEqual(ferry.src.grpc.grpc_server.CLIENT_ID, "new_client_id")
            self.assertEqual(ferry.src.grpc.grpc_server.CLIENT_SECRET, "new_client_secret")

    def test_load_client_secrets_file_not_found(self):
        # Mock the file open to raise FileNotFoundError
        with (
            patch("builtins.open", side_effect=FileNotFoundError),
            patch("logging.warning") as mock_warning,
        ):
            # Call the function
            load_client_secrets()

            # Check that warning was logged
            mock_warning.assert_called_with("No client secrets found. Authentication disabled.")

    def test_validate_request_metadata_valid(self):
        # Set up test data
        request_body = b"test_request_body"
        timestamp = str(int(time.time()))
        expected_signature = hmac.new(
            b"test_client_secret", f"{timestamp}.{request_body.hex()}".encode(), hashlib.sha256
        ).hexdigest()

        metadata = [
            ("x-client-id", "test_client_id"),
            ("x-timestamp", timestamp),
            ("x-signature", expected_signature),
        ]

        # Call the function
        status, error_msg = validate_request_metadata(metadata, request_body)

        # Check that validation passed
        self.assertIsNone(status)
        self.assertIsNone(error_msg)

    def test_validate_request_metadata_missing_headers(self):
        # Test with missing headers
        metadata = [("x-client-id", "test_client_id")]  # Missing timestamp and signature
        request_body = b"test_request_body"

        # Call the function
        status, error_msg = validate_request_metadata(metadata, request_body)

        # Check the expected error
        self.assertEqual(status, grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(error_msg, "Missing authentication headers")

    def test_validate_request_metadata_unknown_client(self):
        # Test with unknown client ID
        timestamp = str(int(time.time()))
        request_body = b"test_request_body"
        metadata = [
            ("x-client-id", "unknown_client"),
            ("x-timestamp", timestamp),
            ("x-signature", "some_signature"),
        ]

        # Call the function
        status, error_msg = validate_request_metadata(metadata, request_body)

        # Check the expected error
        self.assertEqual(status, grpc.StatusCode.UNAUTHENTICATED)
        self.assertEqual(error_msg, "Unknown client ID")

    def test_validate_request_metadata_expired_timestamp(self):
        # Test with expired timestamp (more than 5 minutes old)
        expired_timestamp = str(int(time.time()) - 301)  # 5 minutes + 1 second ago
        request_body = b"test_request_body"
        metadata = [
            ("x-client-id", "test_client_id"),
            ("x-timestamp", expired_timestamp),
            ("x-signature", "some_signature"),
        ]

        # Call the function
        status, error_msg = validate_request_metadata(metadata, request_body)

        # Check the expected error
        self.assertEqual(status, grpc.StatusCode.UNAUTHENTICATED)
        self.assertEqual(error_msg, "Expired timestamp")

    def test_validate_request_metadata_invalid_timestamp(self):
        # Test with invalid timestamp format
        metadata = [
            ("x-client-id", "test_client_id"),
            ("x-timestamp", "not_a_number"),
            ("x-signature", "some_signature"),
        ]
        request_body = b"test_request_body"

        # Call the function
        status, error_msg = validate_request_metadata(metadata, request_body)

        # Check the expected error
        self.assertEqual(status, grpc.StatusCode.UNAUTHENTICATED)
        self.assertEqual(error_msg, "Invalid timestamp format")

    def test_validate_request_metadata_signature_mismatch(self):
        # Test with incorrect signature
        timestamp = str(int(time.time()))
        request_body = b"test_request_body"
        metadata = [
            ("x-client-id", "test_client_id"),
            ("x-timestamp", timestamp),
            ("x-signature", "incorrect_signature"),
        ]

        # Call the function
        status, error_msg = validate_request_metadata(metadata, request_body)

        # Check the expected error
        self.assertEqual(status, grpc.StatusCode.UNAUTHENTICATED)
        self.assertEqual(error_msg, "Signature mismatch")

    @patch("ferry.src.grpc.grpc_server.load_client_secrets")
    @patch("grpc.server")
    def test_serve(self, mock_grpc_server, mock_load_client_secrets):
        # Mock server
        mock_server = MagicMock()
        mock_grpc_server.return_value = mock_server

        # Call serve with test arguments
        serve(port=12345, secure_mode=True)

        # Verify server was started correctly
        mock_load_client_secrets.assert_called_once()
        mock_grpc_server.assert_called_once()
        mock_server.add_insecure_port.assert_called_with("[::]:12345")
        mock_server.start.assert_called_once()
        mock_server.wait_for_termination.assert_called_once()

        # Verify SECURE_MODE was set
        import ferry.src.grpc.grpc_server

        self.assertTrue(ferry.src.grpc.grpc_server.SECURE_MODE)

    @patch("ferry.src.grpc.grpc_server.validate_request_metadata")
    @patch("ferry.src.pipeline_builder.PipelineBuilder")
    def test_ingest_data_secure_mode_validation_failure(self, mock_pipeline_builder, mock_validate):
        # Set secure mode
        import ferry.src.grpc.grpc_server

        ferry.src.grpc.grpc_server.SECURE_MODE = True

        # Mock validation to return an error
        mock_validate.return_value = (grpc.StatusCode.UNAUTHENTICATED, "Auth failed")

        # Create a request
        request = ferry_pb2.IngestRequest(
            identity="test_identity",
            source_uri="test_source_uri",
            destination_uri="test_destination_uri",
            resources=[],
        )

        # Create a context mock
        context = MagicMock()

        # Create the service
        service = FerryServiceServicer()

        # Call the method
        response = service.IngestData(request, context)

        # Check the response
        self.assertEqual(response.status, "ERROR")
        self.assertEqual(response.message, "Auth failed")

        # Verify context was set with error
        context.set_code.assert_called_with(grpc.StatusCode.UNAUTHENTICATED)
        context.set_details.assert_called_with("Auth failed")

        # Verify pipeline builder was NOT called
        mock_pipeline_builder.assert_not_called()


if __name__ == "__main__":
    unittest.main()
