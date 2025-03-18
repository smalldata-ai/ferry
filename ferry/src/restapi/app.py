from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import logging
import os
import json
from celery.result import AsyncResult
from celery import Celery
from ferry.src.restapi.pipeline_utils import load_data_endpoint
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.tasks import load_data_task  
from ferry.src.restapi.config import config  # Import Redis config

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # Ensure logs directory exists

app = FastAPI()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize Celery using config values
celery_app = Celery("tasks", broker=config.CELERY_BROKER_URL, backend=config.CELERY_BACKEND_URL)

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

def get_latest_log_entry(task_id: str):
    """Fetches the last valid JSON log entry from the task-specific JSONL log file."""
    log_file_path = os.path.join(LOG_DIR, f"{task_id}.jsonl")

    if not os.path.exists(log_file_path):
        logger.warning(f"🚨 Log file not found: {log_file_path}")
        return {"message": f"No logs found for task_id {task_id}"}

    try:
        with open(log_file_path, "r", encoding="utf-8") as file:
            lines = file.readlines()

        if not lines:
            logger.warning(f" Log file is empty: {task_id}.jsonl")
            return {"message": "Log file exists but is empty."}

        for line in reversed(lines):  # Read latest log first
            line = line.strip()
            if line:
                try:
                    parsed_log = json.loads(line)
                    logger.info(f" Returning latest log entry: {parsed_log}")
                    return parsed_log  # Return last valid log entry
                except json.JSONDecodeError:
                    logger.error(f" Invalid JSON in logs: {line}")
                    continue

        logger.warning(f" No valid logs found for {task_id}")
        return {"message": "No valid logs found"}
    except Exception as e:
        logger.exception(f" Error reading logs for {task_id}: {e}")
        return {"message": "Error reading logs"}


@app.get("/logs/latest")
def get_latest_log(task_id: str = Query(..., description="Task ID to fetch logs")):
    """API endpoint to retrieve the latest log entry for a given task_id."""
    last_log = get_latest_log_entry(task_id)
    
    if "message" in last_log:
        return JSONResponse(content=last_log, status_code=404)
    
    return JSONResponse(content=last_log)