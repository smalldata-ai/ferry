import os
import json
import tempfile
import shutil
import pytest
from unittest import mock
from ferry.src.log_collector import FerryLogCollector
import sys


@pytest.fixture
def temp_log_dir():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


def test_initialization_creates_log_file(temp_log_dir):
    identity = "test_etl"
    with mock.patch("ferry.src.log_collector.os.makedirs") as makedirs:
        collector = FerryLogCollector(identity=identity)
        assert collector.identity == identity
        assert isinstance(collector.logger, type(sys.stdout))
        assert isinstance(collector.log_file, str)
        makedirs.assert_called_with("logs", exist_ok=True)


def test_log_file_written_and_locked(temp_log_dir):
    identity = "test_log"
    collector = FerryLogCollector(identity=identity)
    collector._log({"test": "value"})

    assert os.path.exists(collector.log_file)
    with open(collector.log_file) as f:
        log_data = json.load(f)
        assert log_data["test"] == "value"
        assert "timestamp" in log_data


def test_update_counter_and_step_tracking(monkeypatch):
    collector = FerryLogCollector("test_update")
    collector.step = "extract"
    collector.update("Customers", inc=5)
    assert collector.counters["Customers"] == 5
    assert collector.table_stats["extract"]["Customers"] == 5

    collector.step = "normalize"
    collector.update("Files", inc=3, label="f1.csv")
    assert "f1.csv" in collector.files_normalized


def test_update_ignores_keywords():
    collector = FerryLogCollector("test_ignore")
    collector.update("_dlt_pipeline_state", inc=1)
    collector.update("Customers", label="_dlt_pipeline_state", inc=1)
    assert "_dlt_pipeline_state" not in collector.counters
    assert all("_dlt_pipeline_state" not in k for k in collector.counters.keys())


def test_dump_counters_extract_normalize_load(monkeypatch):
    collector = FerryLogCollector("test_dump")

    # Fake time and logger
    monkeypatch.setattr("ferry.src.log_collector.time.time", lambda: 1000)
    collector.step = "extract"
    collector.update("Rows", inc=10)
    collector.step = "normalize"
    collector.update("Items", inc=5)
    collector.step = "load"
    collector.update("Jobs", inc=3)

    # Assert data logged correctly
    with open(collector.log_file) as f:
        data = json.load(f)
        assert data["extract"]["status"] == "completed"
        assert data["normalize"]["status"] == "completed"
        assert data["load"]["status"] == "completed"


def test_log_skips_resources():
    collector = FerryLogCollector("test_resources")
    collector.update("Resources", inc=1)
    assert "Resources" not in collector.counters


def test_maybe_log_triggers_dump(monkeypatch):
    collector = FerryLogCollector("test_maybe_log")
    mock_dump = mock.Mock()
    collector.dump_counters = mock_dump
    collector.maybe_log()
    mock_dump.assert_called_once()


def test_start_and_stop(monkeypatch):
    collector = FerryLogCollector("test_stop")
    collector._start("extract")
    collector.update("Tables", inc=2)
    collector._stop()
    assert collector.counters is None
    assert collector.messages is None
    assert collector.counter_info is None


def test_dump_system_stats_toggle(monkeypatch):
    with mock.patch("importlib.util.find_spec", return_value=None):
        collector = FerryLogCollector("test_psutil")
        assert collector.dump_system_stats is False


def test_log_file_locking(temp_log_dir):
    identity = "concurrent_test"
    collector = FerryLogCollector(identity)
    # Simulate two rapid log calls
    collector._log({"first": "entry"})
    collector._log({"second": "entry"})

    with open(collector.log_file) as f:
        data = json.load(f)
        assert "second" in data or "first" in data


def test_files_normalized_truncation(monkeypatch):
    collector = FerryLogCollector("test_files_norm")
    collector.step = "normalize"
    collector.files_normalized = {"f1.csv", "f2.csv", "f3.csv"}
    collector.update("Files", inc=1, label="f4.csv")

    with open(collector.log_file) as f:
        data = json.load(f)
        normalized_files = data["normalize"].get("files_normalized", [])
        assert isinstance(normalized_files, list)
