# Final
from collections import defaultdict
import logging
import os
import sys
# from ferry.src.restapi.collector import LogCollector
from dlt.common.runtime.collector import Collector
import time

import re
from typing import (
    
    DefaultDict,
    Dict,
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

    def __init__(
        self,
        log_period: float = 1.0,
        log_file: str = "ferry_logs.log",  # <-- Add log file
        logger: Union[logging.Logger, TextIO] = sys.stdout,
        log_level: int = logging.INFO,
        dump_system_stats: bool = True,
    ) -> None:
        self.log_period = log_period
        self.logger = logger
        self.log_level = log_level
        self.log_file = log_file  # <-- Store log file path

        # Initialize logging to file
        self.file_logger = logging.getLogger("FerryLog")
        self.file_logger.setLevel(log_level)

        # Add file handler
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        self.file_logger.addHandler(file_handler)

        # Also log to console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        self.file_logger.addHandler(console_handler)

        self.counters = defaultdict(int)
        self.counter_info = {}
        self.messages = {}
        self.last_log_time = None

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
        log_lines = []

        step_header = f" {self.step} ".center(80, "-")
        log_lines.append(step_header)

        for name, count in self.counters.items():
            info = self.counter_info[name]
            elapsed_time = current_time - info.start_time
            items_per_second = (count / elapsed_time) if elapsed_time > 0 else 0

            progress = f"{count}/{info.total}" if info.total else f"{count}"
            percentage = f"({count / info.total * 100:.1f}%)" if info.total else ""
            elapsed_time_str = f"{elapsed_time:.2f}s"
            items_per_second_str = f"{items_per_second:.2f}/s"
            message = f"[{self.messages[name]}]" if self.messages[name] is not None else ""

            #  Dynamically detect and rename the table name
            if ":" in name and not name.startswith(("Processed", "Files", "Items", "Jobs", "Resources")):
                table_name, rest = name.split(":", 1)
                log_lines.append(f"table_records: {rest.strip()}")
            else:
                counter_line = (
                    f"{info.description}: {progress} {percentage} | Time: {elapsed_time_str} | Rate:"
                    f" {items_per_second_str} {message}"
                )
                log_lines.append(counter_line.strip())

        if self.dump_system_stats:
            import psutil
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            current_mem = mem_info.rss / (1024**2)  # Convert to MB
            mem_percent = psutil.virtual_memory().percent
            cpu_percent = process.cpu_percent()
            log_lines.append(
                f"Memory usage: {current_mem:.2f} MB ({mem_percent:.2f}%) | CPU usage:"
                f" {cpu_percent:.2f}%"
            )

        log_lines.append("")
        
        #  Replace any table name dynamically in the final log
        log_message = "\n".join(log_lines)
        log_message = re.sub(r"^\w+_table:", "table_records:", log_message, flags=re.MULTILINE)

        self._log(self.log_level, log_message)


    def _log(self, log_level: int, log_message: str) -> None:
        if isinstance(self.logger, (logging.Logger, logging.LoggerAdapter)):
            self.logger.log(log_level, log_message)
        
        # Write to log file
        self.file_logger.log(log_level, log_message)


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


