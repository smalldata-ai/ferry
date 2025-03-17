from celery import Celery
import logging
import traceback
import os
import json
from ferry.src.restapi.pipeline_utils import load_data_endpoint
from ferry.src.restapi.models import LoadDataRequest
import asyncio
from ferry.src.restapi.config import config
from ferry.src.logging.ferry_log_collector import FerryLogCollector  # Fixed import

LOG_DIR = os.path.abspath("logs")  # Use absolute path for consistency
os.makedirs(LOG_DIR, exist_ok=True)  # Ensure logs directory exists

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

celery_app = Celery("tasks", broker=config.CELERY_BROKER_URL, backend=config.CELERY_BACKEND_URL)

def write_log(task_id, log_data):
    """Write structured logs to a task-specific log file with real-time flush."""
    log_file_path = os.path.join(LOG_DIR, f"{task_id}.jsonl")
    
    # 🔍 Debugging: Print log file path
    print(f"🔍 Writing to log file: {log_file_path}")
    logger.info(f"Writing log entry to: {log_file_path}")

    try:
        with open(log_file_path, "a+", encoding="utf-8") as f:
            f.write(json.dumps(log_data) + "\n")
            f.flush()  # Ensure real-time updates
            os.fsync(f.fileno())  # Force OS-level write to disk
    except Exception as e:
        logger.error(f"🚨 Failed to write log: {e}")

@celery_app.task(bind=True, name="ferry.src.tasks.load_data_task")
def load_data_task(self, request_data: dict):
    """Celery task for loading data asynchronously"""
    request = LoadDataRequest(**request_data)  # Convert dict back to Pydantic model
    task_id = self.request.id  # Extract Celery task ID

    try:
        write_log(task_id, {"status": "started", "message": "Task started"})

        result = asyncio.run(load_data_endpoint(request, task_id))  # ✅ Pass task_id
        
        write_log(task_id, {"status": "success", "result": result})
        return {"status": "success", "result": result}
    except Exception as e:
        error_data = {
            "status": "error",
            "exc_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc(),
        }
        write_log(task_id, error_data)
        return error_data
