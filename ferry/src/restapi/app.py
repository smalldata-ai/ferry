from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import logging
from ferry.src.restapi.pipeline_utils import load_data_endpoint
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse
from ferry.src.tasks import load_data_task  
from celery.result import AsyncResult
from ferry.src.restapi.config import config  # Import Redis config
from celery import Celery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize Celery using config values
celery_app = Celery("tasks", broker=config.CELERY_BROKER_URL, backend=config.CELERY_BACKEND_URL)

@app.post("/load-data", response_model=LoadDataResponse)
async def load_data(request: LoadDataRequest):
    """API endpoint to start data loading asynchronously using Celery"""
    try:
        task = load_data_task.delay(request.model_dump())  # Start Celery task

        logger.info(f"🚀 Task {task.id} started for {request.source_uri} -> {request.destination_uri}")

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
    result = AsyncResult(task_id, app=celery_app)

    if result.state == "PENDING":
        raise HTTPException(status_code=404, detail="Task not found or still pending")

    return {"task_id": task_id, "status": result.state, "result": result.result}
