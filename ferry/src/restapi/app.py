from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

from ferry.src.restapi.pipeline_utils import (
    full_load_endpoint, merge_load_endpoint
)
from ferry.src.restapi.models import (
    LoadDataRequest, LoadDataResponse
)

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}


@app.post("/full-load", response_model=LoadDataResponse)
async def full_load(request: LoadDataRequest):
    """API endpoint to trigger full data loading from Source table to Destination table"""
    try:
        return await full_load_endpoint(request)
    except HTTPException as e:
        logger.error(f"HTTPException in /full-load: {e.detail} (status code: {e.status_code})")
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logger.exception(f"Unexpected error in /full-load: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "message": "An unexpected error occurred."},
        )


@app.post("/merge-load", response_model=LoadDataResponse)
async def merge_load(request: LoadDataRequest):
    """API endpoint to trigger merge incremental loading from Source table to Destination table"""
    try:
        return await merge_load_endpoint(request)
    except HTTPException as e:
        logger.error(f"HTTPException in /merge-load: {e.detail} (status code: {e.status_code})")
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logger.exception(f"Unexpected error in /merge-load: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "message": "An unexpected error occurred."},
        )