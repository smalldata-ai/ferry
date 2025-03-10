# Final
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import logging
import re
import os
from celery.result import AsyncResult
from celery import Celery
from ferry.src.restapi.pipeline_utils import load_data_endpoint
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.tasks import load_data_task  
from ferry.src.restapi.config import config  # Import Redis config

LOG_FILE = "ferry_logs.log"  # 🔹 Log file path

from fastapi.responses import JSONResponse

app = FastAPI()
logging.basicConfig(level=logging.INFO, filename=LOG_FILE, filemode="a", format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize Celery using config values
celery_app = Celery("tasks", broker=config.CELERY_BROKER_URL, backend=config.CELERY_BACKEND_URL)


LOG_FILE_PATH = "ferry_logs.log"  # Update with your actual log file path
# Regex patterns for extracting key details
STAGE_PATTERN = re.compile(
    r"[-]+[\s]*(Extract|Normalize|Load).*?duckdb_to_duckdb[\s]*[-]+", re.IGNORECASE
)
MEMORY_PATTERN = re.compile(r"Memory usage: ([\d.]+) MB \(([\d.]+)%\)")
CPU_PATTERN = re.compile(r"CPU usage: ([\d.]+)%")
FILES_PATTERN = re.compile(r"Files: (\d+)/(\d+) .* Time: ([\d.]+)s \| Rate: ([\d.]+)/s")
ITEMS_PATTERN = re.compile(r"Items: (\d+)  \| Time: ([\d.]+)s \| Rate: ([\d.]+)/s")
PROCESSED_ITEMS_PATTERN = re.compile(r"Processed items: (\d+) \| Rate: ([\d.]+)/s")
PROCESSED_FILES_PATTERN = re.compile(r"Processed files: (\d+)/(\d+)")
RESOURCES_PATTERN = re.compile(r"Resources: (\d+)/(\d+) \(([\d.]+)%\) \| Time: ([\d.]+)s \| Rate: ([\d.]+)/s")
MY_TABLE_PATTERN = re.compile(r"(\w+): (\d+)  \| Time: ([\d.]+)s \| Rate: ([\d.]+)/s")
JOBS_PATTERN = re.compile(r"Jobs: (\d+)/(\d+) \(([\d.]+)%\) \| Time: ([\d.]+)s \| Rate: ([\d.]+)/s")
TABLE_RECORDS_PATTERN = re.compile(r"table_records: (\d+)  \| Time: ([\d.]+)s \| Rate: ([\d.]+)/s")

def get_last_log_entry():
    """Reads only the last log entry from the file."""
    with open(LOG_FILE_PATH, "r") as file:
        log_entries = file.read().strip().split("\n\n")  # Splitting log entries by double newlines

    if not log_entries:
        return {}

    last_entry = log_entries[-1]  # Get only the last log entry
    return parse_log_entry(last_entry)

def parse_log_entry(entry):
    """Parses a single log entry and extracts structured data."""
    log_data = {}

    # Extract timestamp
    timestamp_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)", entry)
    if timestamp_match:
        log_data["timestamp"] = timestamp_match.group(1)

    # Extract stage
    stage_match = STAGE_PATTERN.search(entry)
    if stage_match:
        log_data["stage"] = stage_match.group(1).strip()
    else:
        if "Extract" in entry:
            log_data["stage"] = "Extract"
        elif "Normalize" in entry:
            log_data["stage"] = "Normalize"
        elif "Load" in entry:
            log_data["stage"] = "Load"
        else:
            log_data["stage"] = "Unknown"

    #  Extract table records dynamically
    table_records_match = TABLE_RECORDS_PATTERN.search(entry)
    if table_records_match:
        log_data["table_records"] = {
            "count": int(table_records_match.group(1)),
            "time_sec": float(table_records_match.group(2)),
            "rate_per_sec": float(table_records_match.group(3)),
        }

    # Extract job details
    jobs_match = JOBS_PATTERN.search(entry)
    if jobs_match:
        log_data["jobs"] = {
            "completed": int(jobs_match.group(1)),
            "total": int(jobs_match.group(2)),
            "progress_percent": float(jobs_match.group(3)),
            "time_sec": float(jobs_match.group(4)),
            "rate_per_sec": float(jobs_match.group(5))
        }

    # Extract memory usage
    memory_match = MEMORY_PATTERN.search(entry)
    if memory_match:
        log_data["memory_usage"] = {
            "mb": float(memory_match.group(1)),
            "percentage": float(memory_match.group(2))
        }

    # Extract CPU usage
    cpu_match = CPU_PATTERN.search(entry)
    if cpu_match:
        log_data["cpu_usage_percent"] = float(cpu_match.group(1))

    return log_data

    
@app.post("/load-data", response_model=LoadDataResponse)
async def load_data(request: LoadDataRequest):
    """API endpoint to start data loading asynchronously using Celery"""
    try:
        task = load_data_task.delay(request.model_dump())  # Start Celery task

        logger.info(f" Task {task.id} started for {request.source_uri} -> {request.destination_uri}")

        return {
            "status": "processing",
            "message": "Data loading started in the background.",
            "task_id": task.id,
            "pipeline_name": f"{request.source_uri.split(':')[0]}_to_{request.destination_uri.split(':')[0]}",
            "table_processed": request.destination_table_name
        }
    except Exception as e:
        logger.exception(f" Error starting Celery task: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Failed to start data loading: {str(e)}"}
        )

@app.get("/load-data/status")
def get_task_status(task_id: str):
    """Check Celery task status"""
    try:
        result = AsyncResult(task_id, app=celery_app)

        if result.state == "PENDING":
            return {"task_id": task_id, "status": "pending", "result": None}

        return {"task_id": task_id, "status": result.state, "result": str(result.result)}
    except Exception as e:
        logger.exception(f"Error fetching task status: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching task status: {str(e)}")

@app.get("/logs/latest")
def get_latest_log():
    """Retrieve only the latest log entry in structured JSON format"""
    last_log = get_last_log_entry()
    return JSONResponse(content={"latest_log": last_log})