# main.py
import logging
import os
from fastapi import FastAPI, HTTPException, status
from pydantic import ValidationError
from pipeline_utils import load_data_endpoint
from models import LoadDataRequest, LoadDataResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="DLT Data Pipeline API",
            description="API for managing data pipeline operations",
            version="1.0.0")

CONFIG_PATH = os.environ.get("CONFIG_PATH", "config.yaml")

@app.post("/load_data",
        response_model=LoadDataResponse,
        status_code=status.HTTP_200_OK,
        responses={
            400: {"description": "Invalid request parameters"},
            500: {"description": "Internal server error"}
        })
async def load_data(request: LoadDataRequest):
    try:
        return await load_data_endpoint(request, CONFIG_PATH)
    except ValidationError as e:
        logger.error(f"Validation Error: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )