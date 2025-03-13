# Final
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
import logging
import os
import json
import time
from celery.result import AsyncResult
from celery import Celery
from ferry.src.restapi.pipeline_utils import load_data_endpoint
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.tasks import load_data_task  
from ferry.src.restapi.config import config  # Import Redis config

LOG_FILE_PATH = "ferry_logs.log"

app = FastAPI()
logging.basicConfig(
    level=logging.INFO,
    filename=LOG_FILE_PATH,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize Celery using config values
celery_app = Celery("tasks", broker=config.CELERY_BROKER_URL, backend=config.CELERY_BACKEND_URL)

def get_last_log_entry(task_id: str):
    """Reads the last log entry for a specific task_id from the log file efficiently."""
    try:
        with open(LOG_FILE_PATH, "r") as file:
            log_entries = file.read().split("\n}\n")  # Split entries based on closing brackets

        log_entries = [entry.strip() + "}" for entry in log_entries if entry.strip()]  # Ensure valid JSON format
        
        # Reverse iterate through logs to find the latest entry for task_id
        for entry in reversed(log_entries):
            try:
                log_data = json.loads(entry)  # Parse full JSON object
                if log_data.get("task_id") == task_id:
                    return {"latest_log": log_data}  # Return the full log entry
            except json.JSONDecodeError:
                continue  # Skip invalid JSON entries

        return {"message": "No logs found for this task_id"}  # Return if no match found

    except Exception as e:
        logger.exception(f"Error reading logs: {e}")
        return {"message": "Error reading logs"}

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
def get_latest_log(task_id: str):
    """Retrieve the latest log entry for a given task_id in structured JSON format"""
    last_log = get_last_log_entry(task_id)
    
    if not last_log or "latest_log" not in last_log:
        return JSONResponse(content={"message": "No logs found for this task_id"}, status_code=404)
    
    return JSONResponse(content=last_log)

# 🔥 Real-Time Log Streaming Endpoint
def log_stream(task_id: str):
    """Generator function to stream logs in real time."""
    with open(LOG_FILE_PATH, "r") as file:
        file.seek(0, os.SEEK_END)  # Move to the end of file to get only new logs
        while True:
            line = file.readline()  # Read new line
            if line:
                try:
                    log_data = json.loads(line.strip())  # Parse JSON
                    if log_data.get("task_id") == task_id:
                        yield f"data: {json.dumps(log_data)}\n\n"  # SSE format
                except json.JSONDecodeError:
                    continue  # Skip invalid logs
            
            time.sleep(1)  # Avoid busy waiting

@app.get("/logs/stream")
def stream_logs(task_id: str):
    """API endpoint to stream logs in real-time for a given task_id."""
    return StreamingResponse(log_stream(task_id), media_type="text/event-stream")
