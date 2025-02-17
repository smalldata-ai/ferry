from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse


from src.restapi.pipeline_utils import load_data_endpoint
from src.restapi.models import LoadDataRequest, LoadDataResponse

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}


@app.post("/load-data", response_model=LoadDataResponse)
async def load_data(request: LoadDataRequest):
    """API endpoint to trigger data loading from PostgreSQL table to ClickHouse table"""
    try:
        return await load_data_endpoint(request)
    except HTTPException as e:
        logger.error(f"HTTPException in /load-data: {e.detail} (status code: {e.status_code})")
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logger.exception(f"Unexpected error in /load-data: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "message": "An unexpected error occurred."},
        )