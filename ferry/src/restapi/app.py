from http.client import HTTPException
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse

from ferry.src.pipeline import Pipeline
from ferry.src.restapi.pipeline_utils import (
    full_load_endpoint, merge_load_endpoint
)
from ferry.src.restapi.models import (
    LoadDataRequest, LoadDataResponse, LoadStatus
)
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
        pipeline = Pipeline(
            source_uri=request.source_uri,
            source_table=request.source_table_name,
            destination_uri=request.destination_uri,
            destination_table=request.destination_table_name
        )
        
        pipeline.build().run()
        task_status[dataset_key] = "success"  # Mark success
        # logger.info(f" Background task completed successfully: {result}")
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


@app.post("/ingest", response_model=LoadDataResponse)
def full_load(request: LoadDataRequest,  background_tasks: BackgroundTasks):
    """API endpoint to trigger full data loading from Source table to Destination table"""
    try:
        
        background_tasks.add_task(background_load_data, request)  #  Use BackgroundTasks

        return {
            "status": "processing",
            "message": "Data loading started in the background.",
            # "pipeline_name": f"{request.source_uri.split(':')[0]}_to_{request.destination_uri.split(':')[0]}",
            "table_processed": request.destination_table_name
        }
        # return await full_load_endpoint(request)
    except Exception as e:
        logger.exception(f" Error starting background task: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Failed to start data loading: {str(e)}"}
        )



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
    
# @app.post("/merge-load", response_model=LoadDataResponse)
# async def merge_load(request: LoadDataRequest):
#     """API endpoint to trigger merge incremental loading from Source table to Destination table"""
#     try:
#         return await merge_load_endpoint(request)
#     except HTTPException as e:
#         logger.error(f"HTTPException in /merge-load: {e.detail} (status code: {e.status_code})")
#         return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
#     except Exception as e:
#         logger.exception(f"Unexpected error in /merge-load: {e}")    

# async def background_load_data(request: LoadDataRequest):
#     """Runs data loading in the background and updates status"""
#     dataset_key = f"{request.source_uri}_{request.destination_uri}"
#     logger.info(f"🚀 Starting background task for: {dataset_key}")
    
#     task_status[dataset_key] = "running"  # Mark as running

#     try:
#         result = await load_data_endpoint(request)  # Run data loading
#         task_status[dataset_key] = "success"  # Mark success
#         logger.info(f" Background task completed successfully: {result}")
#     except Exception as e:
#         task_status[dataset_key] = f"error: {str(e)}"  # Store error
#         logger.exception(f" Background task failed: {e}")

