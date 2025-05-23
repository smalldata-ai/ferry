# Final
from collections import defaultdict
import logging
import sys
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

# logger = logging.getLogger(__name__)
class FerryLogCollector(Collector):
    """A Collector that shows progress by writing to a Python logger or a console"""

    logger: Union[logging.Logger, TextIO]
    log_level: int

    class CounterInfo(NamedTuple):
        description: str
        start_time: float
        total: Optional[int]

    def __init__(self, task_id: str, log_period: float = 1.0,
                 logger: Union[logging.Logger, TextIO] = sys.stdout, log_level: int = logging.INFO,
                 dump_system_stats: bool = True) -> None:
        self.task_id = task_id
        self.log_period = log_period
        self.logger = logger
        self.log_level = log_level

        # Ensure logs directory exists
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        self.log_file = os.path.join(log_dir, f"{task_id}.jsonl")

        self.counters = defaultdict(int)
        self.counter_info = {}
        self.messages = {}
        self.last_log_time = None
        self.last_in_process = {}
        self.completed_logs = {}

        self.etl_status = {
            "extract": {"status": "pending"},
            "normalize": {"status": "pending"},
            "load": {"status": "pending"},
        }

        if dump_system_stats:
            try:
                import psutil
            except ImportError:
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
    # Ignore unwanted tables
        if name in ["_dlt_pipeline_state", "Resources"]:
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
                if "extract" in self.step.lower():
                    log_entry.update({"records_extracted": count})
                    self.last_in_process["extract"] = log_entry
                    log_data["extract"] = log_entry.copy()
                elif "normalize" in self.step.lower():
                    log_data["extract"]["status"] = "completed"
                    log_entry.update({"files_processed": count})
                    self.last_in_process["normalize"] = log_entry
                    log_data["normalize"] = log_entry.copy()
                elif "load" in self.step.lower():
                    log_data["extract"]["status"] = "completed"
                    log_data["normalize"]["status"] = "completed"
                    log_entry.update({"jobs_loaded": count})
                    self.last_in_process["load"] = log_entry
                    log_data["load"] = log_entry.copy()

        self._log(log_data)


    def _log(self, log_message: dict) -> None:
        """Write logs in JSONL format."""
        with open(self.log_file, "w+", encoding="utf-8") as log_file:
            log_file.write(json.dumps(log_message) + "\n")



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