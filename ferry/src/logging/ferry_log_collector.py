# Final
from collections import defaultdict
import logging
import sys
# from ferry.src.restapi.collector import LogCollector
from dlt.common.runtime.collector import Collector
import time
import json
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

    def __init__(self, task_id: str, log_period: float = 1.0, log_file: str = "ferry_logs.log",
                 logger: Union[logging.Logger, TextIO] = sys.stdout, log_level: int = logging.INFO,
                 dump_system_stats: bool = True) -> None:
        self.task_id = task_id  # Store task_id
        self.log_period = log_period
        self.logger = logger
        self.log_level = log_level
        self.log_file = log_file

        # Initialize logging to file
        self.file_logger = logging.getLogger("FerryLog")
        self.file_logger.setLevel(log_level)

        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        self.file_logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        self.file_logger.addHandler(console_handler)

        self.counters = defaultdict(int)
        self.counter_info = {}
        self.messages = {}
        self.last_log_time = None

        # Track ETL progress
        self.etl_status = {
            "extract": {"status": "pending"},
            "normalize": {"status": "pending"},
            "load": {"status": "pending"},
        }

        if dump_system_stats:
            try:
                import psutil
            except ImportError:
                self._log(logging.WARNING, "psutil is missing, system stats won't be logged.")
                dump_system_stats = False

        self.dump_system_stats = dump_system_stats

    
    def update(
    self,
    name: str,
    inc: int = 1,
    total: int = None,
    inc_total: int = None,  # <-- Add this
    message: str = None,
    label: str = None,
    batch_size: int = None,
) -> None:
        counter_key = f"{name}_{label}" if label else name

        if counter_key not in self.counters:
            self.counters[counter_key] = 0
            self.counter_info[counter_key] = FerryLogCollector.CounterInfo(
                description=f"{name} ({label})" if label else name,
                start_time=time.time(),
                total=total,
            )
            self.messages[counter_key] = None

        self.counters[counter_key] += inc

        # Handle inc_total if provided
        if message:
            self.messages[counter_key] = message

    # 🚀 Log immediately
        self.dump_counters()


    def maybe_log(self) -> None:
        """Force logging on every update to get real-time row-level details."""
        self.dump_counters()


    def dump_counters(self) -> None:
        current_time = time.time()
        
        # Default structure for logs
        log_data = {
            "extract": {
                "status": "pending"
            },
            "normalize": {
                "status": "pending"
            },
            "load": {
                "status": "pending"
            }
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

            # Updating based on processing step
            if "extract" in self.step.lower():
                log_entry.update({
                    "records_extracted": count,
                    "percentage": round((count / info.total) * 100, 2) if info.total else None,
                    "resource_type": "Resources",
                    "resource_count": 0
                })
                log_data["extract"] = log_entry

            elif "normalize" in self.step.lower():
                # Mark Extract as completed before starting Normalize
                if "extract" in log_data:
                    log_data["extract"]["status"] = "completed"

                log_entry.update({
                    "files": f"{count}/{info.total}" if info.total else f"{count}/?",
                    "time": f"{elapsed_time:.2f}s",
                    "rate": f"{items_per_second:.2f}/s",
                })
                log_data["normalize"] = log_entry

            elif "load" in self.step.lower():
                # Mark Extract and Normalize as completed before starting Load
                if "extract" in log_data:
                    log_data["extract"]["status"] = "completed"
                if "normalize" in log_data:
                    log_data["normalize"]["status"] = "completed"

                log_entry.update({
                    "jobs": f"{count}/{info.total}" if info.total else f"{count}/?",
                    "time": f"{elapsed_time:.2f}s",
                    "rate": f"{items_per_second:.2f}/s",
                })
                log_data["load"] = log_entry

        json_log = json.dumps(log_data, indent=4)
        self._log(self.log_level, json_log)

    def _log(self, log_level: int, log_message: Union[str, dict]) -> None:
        """Ensures JSON logs are written cleanly without duplication"""
        if isinstance(log_message, dict):  # If it's a dict, convert to JSON
            log_message = json.dumps(log_message, indent=4)

        if isinstance(self.logger, (logging.Logger, logging.LoggerAdapter)):
            self.logger.log(log_level, log_message)  # Logs directly without prefix

        self.file_logger.handlers.clear()  # Remove existing handlers to avoid duplicate logs

        file_handler = logging.FileHandler(self.log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(message)s"))  # Raw JSON, no prefix
        self.file_logger.addHandler(file_handler)

        self.file_logger.log(log_level, log_message)  # Log raw JSON




    def _start(self, step: str) -> None:
        if not self.counters:  # Don't reset if already initialized
            self.counters = defaultdict(int)
            self.counter_info = {}
            self.messages = {}
        self.last_log_time = time.time()

    def _stop(self) -> None:
        self.dump_counters()
        self.counters = None
        self.counter_info = None
        self.messages = None
        self.last_log_time = None


