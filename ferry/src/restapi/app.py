from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
import logging
from ferry.src.restapi.pipeline_utils import load_data_endpoint
from ferry.src.restapi.models import LoadDataRequest, LoadDataResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

task_status = {}  # Dictionary to track task results

async def background_load_data(request: LoadDataRequest):
    """Runs data loading in the background and updates status"""
    dataset_key = f"{request.source_uri}_{request.destination_uri}"
    logger.info(f"🚀 Starting background task for: {dataset_key}")
    
    task_status[dataset_key] = "running"  # Mark as running

    try:
        result = await load_data_endpoint(request)  # Run data loading
        task_status[dataset_key] = "success"  # Mark success
        logger.info(f" Background task completed successfully: {result}")
    except Exception as e:
        task_status[dataset_key] = f"error: {str(e)}"  # Store error
        logger.exception(f" Background task failed: {e}")


@app.get("/load-data/status")
async def get_status(source_uri: str, destination_uri: str):
    """Check the status of a background data load task"""
    dataset_key = f"{source_uri}_{destination_uri}"
    
    if dataset_key not in task_status:
        logger.warning(f" Status check for unknown dataset: {dataset_key}")
        return {"source_uri": source_uri, "destination_uri": destination_uri, "status": "unknown"}

    status = task_status[dataset_key]
    logger.info(f"ℹ️ Status for {dataset_key}: {status}")
    return {"source_uri": source_uri, "destination_uri": destination_uri, "status": status}


@app.post("/load-data", response_model=LoadDataResponse)
async def load_data(request: LoadDataRequest, background_tasks: BackgroundTasks):
    """API endpoint to start background data loading"""
    try:
        background_tasks.add_task(background_load_data, request)  #  Use BackgroundTasks

        return {
            "status": "processing",
            "message": "Data loading started in the background.",
            "pipeline_name": f"{request.source_uri.split(':')[0]}_to_{request.destination_uri.split(':')[0]}",
            "table_processed": request.destination_table_name
        }
    except Exception as e:
        logger.exception(f" Error starting background task: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Failed to start data loading: {str(e)}"}
        )
