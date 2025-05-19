import pytest
import time
import json
import hmac
import hashlib
from unittest import mock
from grpc import StatusCode
from ferry.src.grpc import grpc_server
from ferry.src.grpc.grpc_server import FerryServiceServicer


@pytest.fixture(autouse=True)
def reset_auth_globals():
    grpc_server.CLIENT_ID = "test-client"
    grpc_server.CLIENT_SECRET = "test-secret"
    grpc_server.SECURE_MODE = False


def generate_signature(timestamp, body, secret):
    message = f"{timestamp}.{body.hex()}".encode()
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def test_load_client_secrets_success(tmp_path):
    secrets_path = tmp_path / ".ferry" / "server_secrets.json"
    secrets_path.parent.mkdir(parents=True)
    secrets_path.write_text(json.dumps({"client_id": "abc", "client_secret": "xyz"}))

    with mock.patch("ferry.src.grpc.grpc_server.os.path.join", return_value=str(secrets_path)):
        grpc_server.load_client_secrets()
        assert grpc_server.CLIENT_ID == "abc"
        assert grpc_server.CLIENT_SECRET == "xyz"


def test_load_client_secrets_file_not_found():
    with mock.patch("builtins.open", side_effect=FileNotFoundError):
        grpc_server.load_client_secrets()  # Should not raise


def test_validate_request_metadata_success():
    body = b"hello-world"
    timestamp = str(int(time.time()))
    signature = generate_signature(timestamp, body, grpc_server.CLIENT_SECRET)
    metadata = [
        ("x-client-id", grpc_server.CLIENT_ID),
        ("x-timestamp", timestamp),
        ("x-signature", signature),
    ]
    status, msg = grpc_server.validate_request_metadata(metadata, body)
    assert status is None
    assert msg is None


def test_validate_request_metadata_missing_headers():
    metadata = [("x-client-id", grpc_server.CLIENT_ID)]
    status, msg = grpc_server.validate_request_metadata(metadata, b"body")
    assert status == StatusCode.INVALID_ARGUMENT
    assert "Missing" in msg


def test_validate_request_metadata_invalid_client_id():
    metadata = [("x-client-id", "wrong"), ("x-timestamp", "123"), ("x-signature", "abc")]
    status, msg = grpc_server.validate_request_metadata(metadata, b"body")
    assert status == StatusCode.UNAUTHENTICATED
    assert "Unknown" in msg


def test_validate_request_metadata_expired_timestamp():
    old_ts = str(int(time.time()) - 600)
    sig = generate_signature(old_ts, b"abc", grpc_server.CLIENT_SECRET)
    metadata = [
        ("x-client-id", grpc_server.CLIENT_ID),
        ("x-timestamp", old_ts),
        ("x-signature", sig),
    ]
    status, msg = grpc_server.validate_request_metadata(metadata, b"abc")
    assert status == StatusCode.UNAUTHENTICATED
    assert "Expired" in msg


def test_validate_request_metadata_invalid_timestamp():
    metadata = [
        ("x-client-id", grpc_server.CLIENT_ID),
        ("x-timestamp", "abc"),
        ("x-signature", "xyz"),
    ]
    status, msg = grpc_server.validate_request_metadata(metadata, b"abc")
    assert status == StatusCode.UNAUTHENTICATED
    assert "timestamp format" in msg


def test_validate_request_metadata_signature_mismatch():
    timestamp = str(int(time.time()))
    metadata = [
        ("x-client-id", grpc_server.CLIENT_ID),
        ("x-timestamp", timestamp),
        ("x-signature", "bad-sign"),
    ]
    status, msg = grpc_server.validate_request_metadata(metadata, b"abc")
    assert status == StatusCode.UNAUTHENTICATED
    assert "Signature mismatch" in msg


def test_ingest_data_success():
    mock_context = mock.Mock()
    mock_pipeline = mock.Mock()
    mock_pipeline.get_name.return_value = "test-pipeline"

    with mock.patch("ferry.src.grpc.grpc_server.PipelineBuilder") as pb:
        pb.return_value.build.return_value = mock_pipeline
        servicer = FerryServiceServicer()
        request = mock.Mock()
        request.identity = "abc"
        request.source_uri = "source"
        request.destination_uri = "dest"
        request.resources = []

        with mock.patch("ferry.src.grpc.grpc_server.IngestModel") as MockModel:
            MockModel.return_value = mock.Mock()
            result = servicer.IngestData(request, mock_context)
            assert result.status == "SUCCESS"
            assert result.pipeline_name == "test-pipeline"


def test_ingest_data_auth_failure():
    grpc_server.SECURE_MODE = True
    servicer = FerryServiceServicer()
    request = mock.Mock()
    request.SerializeToString.return_value = b"body"
    mock_context = mock.Mock()
    mock_context.invocation_metadata.return_value = []
    result = servicer.IngestData(request, mock_context)
    assert result.status == "ERROR"
    mock_context.set_code.assert_called_once()


def test_ingest_data_exception():
    mock_pipeline_builder = mock.Mock()
    mock_pipeline_builder.build.side_effect = Exception("fail")

    with mock.patch(
        "ferry.src.grpc.grpc_server.PipelineBuilder", return_value=mock_pipeline_builder
    ):
        with mock.patch("ferry.src.grpc.grpc_server.IngestModel", autospec=True) as MockModel:
            mock_ingest_model = mock.Mock()
            MockModel.return_value = mock_ingest_model
            servicer = FerryServiceServicer()
            request = mock.Mock()
            request.identity = "abc"
            request.source_uri = "source"
            request.destination_uri = "dest"
            request.resources = []
            mock_context = mock.Mock()
            result = servicer.IngestData(request, mock_context)
            assert result.status == "ERROR"
            assert "fail" in result.message


def test_get_observability_success():
    mock_metrics = {
        "pipeline_name": "test_pipeline",
        "start_time": "2024-04-30T10:00:00Z",
        "end_time": "2024-04-30T10:05:00Z",
        "status": "success",
        "destination_type": "postgres",
        "source_type": "s3",
        "error": None,
        "metrics": {
            "extract": {"records": 100},
            "normalize": {"records": 100},
            "load": {"records": 100},
        },
    }

    with mock.patch(
        "ferry.src.grpc.grpc_server.PipelineMetrics.generate_metrics",
        return_value=mock_metrics,
    ):
        servicer = FerryServiceServicer()
        request = mock.Mock()
        request.identity = "abc"
        context = mock.Mock()
        resp = servicer.GetObservability(request, context)
        assert resp.status == "SUCCESS"


def test_get_observability_auth_failure():
    grpc_server.SECURE_MODE = True
    servicer = FerryServiceServicer()
    request = mock.Mock()
    request.SerializeToString.return_value = b"body"
    context = mock.Mock()
    context.invocation_metadata.return_value = []
    resp = servicer.GetObservability(request, context)
    assert resp.status == "ERROR"


def test_get_observability_exception():
    with mock.patch(
        "ferry.src.grpc.grpc_server.PipelineMetrics.generate_metrics", side_effect=Exception("fail")
    ):
        servicer = FerryServiceServicer()
        request = mock.Mock()
        request.identity = "abc"
        context = mock.Mock()
        resp = servicer.GetObservability(request, context)
        assert resp.status == "ERROR"
        assert "fail" in context.set_details.call_args[0][0]


def test_serve_starts_server():
    with (
        mock.patch("ferry.src.grpc.grpc_server.grpc.server") as mock_server,
    ):
        mock_srv_instance = mock_server.return_value
        grpc_server.serve(5050, secure_mode=True)
        mock_server.assert_called_once()
        mock_srv_instance.start.assert_called_once()
        mock_srv_instance.wait_for_termination.assert_called_once()
