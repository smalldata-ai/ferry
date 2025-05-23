# Final
from collections import defaultdict
import logging
import shutil
import sys
import tempfile
from dlt.common.runtime.collector import Collector
import time
import json
import os
from typing import (
    NamedTuple,
    Optional,
    TextIO,
    Union,
)
import importlib.util
from filelock import FileLock
import threading


# logger = logging.getLogger(__name__)
class FerryLogCollector(Collector):
    """A Collector that shows progress by writing to a Python logger or a console"""

    logger: Union[logging.Logger, TextIO]
    log_level: int
    log_lock = threading.Lock()
    last_good_log = {}

    class CounterInfo(NamedTuple):
        description: str
        start_time: float
        total: Optional[int]

    def __init__(
        self,
        identity: str,
        log_period: float = 1.0,
        logger: Union[logging.Logger, TextIO] = sys.stdout,
        log_level: int = logging.INFO,
        dump_system_stats: bool = True,
    ) -> None:
        self.identity = identity
        self.log_period = log_period
        self.logger = logger
        self.log_level = log_level
        self.files_normalized = set()

        # Ensure logs directory exists
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        self.log_file = os.path.join(log_dir, f"{identity}.jsonl")

        self.counters = defaultdict(int)
        self.counter_info = {}
        self.messages = {}
        self.last_log_time = None
        self.last_in_process = {}
        self.completed_logs = {}
        self.normalized_file_stats = defaultdict(int)

        self.etl_status = {
            "extract": {"status": "pending"},
            "normalize": {"status": "pending"},
            "load": {"status": "pending"},
        }

        self.table_stats = {
            "extract": defaultdict(int),
            "normalize": defaultdict(int),
            "load": defaultdict(int),
        }

        if dump_system_stats:
            if importlib.util.find_spec("psutil") is None:
                self._log({"warning": "psutil is missing, system stats won't be logged."})
                dump_system_stats = False

        self.dump_system_stats = dump_system_stats

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = None,
        batch_size: int = None,
    ) -> None:
        if name in {"Resources"}:
            return

        ignored_keywords = {"_dlt_pipeline_state"}

        if any(ignored in name for ignored in ignored_keywords):
            return
        if label and any(ignored in label for ignored in ignored_keywords):
            return

        counter_key = f"{name}_{label}" if label else name

        if counter_key not in self.counters:
            self.counters[counter_key] = 0
            self.counter_info[counter_key] = FerryLogCollector.CounterInfo(
                description=f"{name} ({label})" if label else name,
                start_time=time.time(),
                total=total,
            )
            self.messages[counter_key] = None

        print(f"Logging update for {name}: {inc}, total count: {self.counters.get(name, 0)}")

        self.counters[counter_key] += inc
        if hasattr(self, "step") and isinstance(self.step, str):
            current_step = self.step.lower()
            if "extract" in current_step:
                self.table_stats["extract"][name] += inc
            elif "normalize" in current_step:
                self.table_stats["normalize"][name] += inc
                if name == "Files" and label:
                    self.files_normalized.add(label)
            elif "load" in current_step:
                self.table_stats["load"][name] += inc

        if message:
            self.messages[counter_key] = message

        self.dump_counters()

    def maybe_log(self) -> None:
        """Force logging on every update to get real-time row-level details."""
        self.dump_counters()

    def dump_counters(self) -> None:
        current_time = time.time()
        log_data = {
            "extract": self.last_in_process.get("extract", {"status": "pending"}).copy(),
            "normalize": self.last_in_process.get("normalize", {"status": "pending"}).copy(),
            "load": self.last_in_process.get("load", {"status": "pending"}).copy(),
        }

        for name, count in self.counters.items():
            info = self.counter_info[name]
            elapsed_time = round(current_time - info.start_time, 2)
            items_per_second = round(count / elapsed_time, 2) if elapsed_time > 0 else 0

            log_entry = {
                "elapsed_time": elapsed_time,
                "rate": items_per_second,
                "status": "in-process",
            }

            if hasattr(self, "step") and isinstance(self.step, str):
                step_lower = self.step.lower()

                if "extract" in step_lower:
                    log_entry.update(
                        {
                            "records_extracted": count,
                            "table_stats": dict(self.table_stats["extract"]),
                        }
                    )

                    self.last_in_process["extract"] = log_entry
                    log_data["extract"] = log_entry.copy()

                elif "normalize" in step_lower:
                    log_data["extract"]["status"] = "completed"
                    normalize_stats = {}
                    for k, v in self.table_stats["normalize"].items():
                        if k in {"Files", "Items"} and v > 0:
                            normalize_stats[k] = v - 1
                        else:
                            normalize_stats[k] = v
                    log_entry.update(
                        {
                            "records_extracted": max(count - 1, 0),
                            "table_stats": normalize_stats,
                            "files_normalized": sorted(list(self.files_normalized)[:-1])
                            if self.files_normalized
                            else [],
                        }
                    )

                    self.last_in_process["normalize"] = log_entry
                    log_data["normalize"] = log_entry.copy()

                elif "load" in step_lower:
                    log_data["extract"]["status"] = "completed"
                    log_data["normalize"]["status"] = "completed"
                    load_stats = {
                        k: (v - 1 if k.lower() == "jobs" and v > 0 else v)
                        for k, v in self.table_stats["load"].items()
                    }
                    log_entry.update(
                        {
                            "records_extracted": max(count - 1, 0),
                            "table_stats": load_stats,
                        }
                    )
                    log_entry["status"] = "completed"

                    self.last_in_process["load"] = log_entry
                    log_data["load"] = log_entry.copy()

        self._log(log_data)

    # def _log(self, log_message: dict) -> None:
    #     """Overwrite the log file with a single latest JSON object."""
    #     log_message["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #     with open(self.log_file, "w", encoding="utf-8") as log_file:
    #         log_file.write(json.dumps(log_message, indent=2))  # Optional pretty-print

    def _log(self, log_message: dict) -> None:
        if "_dlt_pipeline_state" in json.dumps(log_message):
            print("[DEBUG] _dlt_pipeline_state made it to log!")
        """Safely write JSON log with locking to avoid access conflicts (cross-platform safe)."""
        log_message["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        dir_name = os.path.dirname(self.log_file)
        temp_log_fd, temp_log_path = tempfile.mkstemp(dir=dir_name, suffix=".jsonl")

        try:
            # Write the log message to a temp file
            with os.fdopen(temp_log_fd, "w", encoding="utf-8") as tmp_file:
                json.dump(log_message, tmp_file, indent=2)

            # Lock the main file to avoid race conditions
            lock_path = f"{self.log_file}.lock"
            with FileLock(lock_path, timeout=5):
                shutil.move(temp_log_path, self.log_file)

        except Exception as e:
            # Clean up temp file on failure
            if os.path.exists(temp_log_path):
                os.remove(temp_log_path)
            raise e

    def _start(self, step: str) -> None:
        if not self.counters:  # Don't reset if already initialized
            self.counters = defaultdict(int)
            self.counter_info = {}
            self.messages = {}
        self.last_log_time = time.time()

    def _stop(self) -> None:
        """Ensures the latest in-process metrics are logged before shutting down."""
        self.dump_counters()  # Log the last recorded metrics

        # Reset internal states
        self.counters = None
        self.counter_info = None
        self.messages = None
        self.last_log_time = None
