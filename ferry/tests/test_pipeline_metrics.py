import json
from unittest import mock
from ferry.src.pipeline_metrics import PipelineMetrics


def test_pipeline_metrics_init():
    with mock.patch("ferry.src.pipeline_metrics.dlt.pipeline") as mock_pipeline:
        mock_pipeline.return_value.last_trace = "last-trace"
        metrics = PipelineMetrics("test-pipeline")
        assert metrics.pipeline_name == "test-pipeline"
        assert metrics.pipeline == mock_pipeline.return_value
        assert metrics.last_trace == "last-trace"
        assert isinstance(metrics.metrics, dict)


def test_default_metrics_with_error():
    metrics = PipelineMetrics("test-pipeline")
    result = metrics._default_metrics(error="some error")
    assert result["status"] == "error"
    assert result["error"] == "some error"
    for step in ["extract", "normalize", "load"]:
        assert result["metrics"][step]["status"] == "pending"


def test_load_live_metrics_from_log_success():
    log_content = {
        "extract": {"status": "completed", "table_stats": {"table1": 123}},
        "normalize": {"status": "completed", "files_normalized": ["file1.parquet"]},
        "load": {"status": "pending"},
    }

    with (
        mock.patch(
            "ferry.src.pipeline_metrics.open", mock.mock_open(read_data=json.dumps(log_content))
        ),
        mock.patch("ferry.src.pipeline_metrics.json.load", return_value=log_content),
        mock.patch("ferry.src.pipeline_metrics.dlt.pipeline") as mock_pipeline,
    ):
        mock_pipeline.return_value.last_trace = None
        metrics = PipelineMetrics("test-pipeline")
        test_metrics = {
            "metrics": {
                "extract": {"resource_metrics": []},
                "normalize": {"resource_metrics": []},
                "load": {"resource_metrics": []},
            }
        }
        metrics._load_live_metrics_from_log(test_metrics)
        assert test_metrics["status"] == "processing"
        assert test_metrics["metrics"]["extract"]["status"] == "completed"
        assert {"name": "table1", "row_count": 123} in test_metrics["metrics"]["extract"][
            "resource_metrics"
        ]
        assert {"name": "file1.parquet", "type": "normalized_file"} in test_metrics["metrics"][
            "normalize"
        ]["resource_metrics"]


def test_load_live_metrics_from_log_file_not_found():
    metrics = PipelineMetrics("test-pipeline")
    dummy_metrics = {"metrics": {"extract": {}, "normalize": {}, "load": {}}}
    with mock.patch("builtins.open", side_effect=FileNotFoundError):
        metrics._load_live_metrics_from_log(dummy_metrics)
        assert dummy_metrics["status"] == "error"
        assert "Live log error" in dummy_metrics["error"]


def test_generate_metrics_pipeline_activation_fail():
    with mock.patch("ferry.src.pipeline_metrics.dlt.pipeline") as mock_pipeline:
        mock_pipeline.return_value.activate.side_effect = Exception("not found")
        metrics = PipelineMetrics("test-pipeline")
        result = metrics.generate_metrics()
        assert result["status"] == "error"
        assert "not found" in result["error"]


def test_generate_metrics_with_trace():
    mock_trace = mock.Mock()
    mock_trace.started_at = "2024-01-01T00:00:00Z"
    mock_trace.finished_at = "2024-01-01T01:00:00Z"
    mock_trace.steps = []

    with mock.patch("ferry.src.pipeline_metrics.dlt.pipeline") as mock_pipeline:
        mock_pipeline.return_value._trace = mock_trace
        mock_pipeline.return_value.activate = mock.Mock()
        metrics = PipelineMetrics("test-pipeline")
        result = metrics.generate_metrics()
        assert result["status"] in ["completed", "processing", "failed", "unknown"]
        assert result["start_time"] is not None


def test_add_extract_metrics():
    metrics = PipelineMetrics("test-pipeline")
    mock_resource_metric = mock.Mock(items_count=100, file_size=2048)
    extract_info = mock.Mock()
    extract_info.metrics = {"load1": [{"resource_metrics": {"table1": mock_resource_metric}}]}
    metrics_dict = {"resource_metrics": []}
    metrics._add_extract_metrics(extract_info, metrics_dict)
    assert {"name": "table1", "row_count": 100, "file_size": 2048} in metrics_dict[
        "resource_metrics"
    ]


def test_add_normalize_metrics():
    metrics = PipelineMetrics("test-pipeline")
    table_metric = mock.Mock(items_count=50, file_size=512)
    normalize_info = mock.Mock()
    normalize_info.metrics = {"load2": [{"table_metrics": {"table2": table_metric}}]}
    metrics_dict = {"resource_metrics": []}
    metrics._add_normalize_metrics(normalize_info, metrics_dict)
    assert {"name": "table2", "row_count": 50, "file_size": 512} in metrics_dict["resource_metrics"]


def test_add_load_metrics():
    metrics = PipelineMetrics("test-pipeline")
    job_metric = mock.Mock(
        table_name="table3", started_at="2024-01-01T00:00:00Z", finished_at="2024-01-01T00:30:00Z"
    )
    load_info = mock.Mock()
    load_info.metrics = {"load3": [{"job_metrics": {"job123": job_metric}}]}
    metrics_dict = {"resource_metrics": []}
    metrics._add_load_metrics(load_info, metrics_dict)
    assert metrics_dict["resource_metrics"][0]["name"] == "table3"
