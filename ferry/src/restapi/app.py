import logging
from http.client import HTTPException
import os
from fastapi import FastAPI,Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import yaml

from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.data_models.response_models import IngestResponse, LoadStatus
from ferry.src.pipeline_builder import PipelineBuilder


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

last_sent_schema_hashes = {}

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    error_dict = {}
    for error in exc.errors():
        field = error['loc'][-1]  
        message = error['msg']  
        if field not in error_dict:
            error_dict[field] = []
        error_dict[field].append(message)

    return JSONResponse(
        status_code=422,
        content={"errors": error_dict}
    )

@app.post("/ingest", response_model=IngestResponse)
def ingest(ingest_model: IngestModel):
    """API endpoint to trigger ingesting data from source to destination"""
    try:
        pipeline = PipelineBuilder(model=ingest_model).build()
        pipeline.run()

        schema_file = f"{ingest_model.identity}.schema.yaml"
        schema_path = os.path.join(".schemas", schema_file)

        schema_data = None
        schema_version_hash = None
        schema_status = None

        if os.path.exists(schema_path):
            with open(schema_path, "r") as f:
                schema_data = yaml.safe_load(f)
            schema_version_hash = schema_data.get("version_hash", "")
            last_hash = last_sent_schema_hashes.get(schema_file, "")

            if schema_version_hash != last_hash:
                schema_status = "updated"
                last_sent_schema_hashes[schema_file] = schema_version_hash
            else:
                schema_status = "unchanged"
                schema_data = None

        return IngestResponse(
            status=LoadStatus.SUCCESS.value,
            message="Data Ingestion is completed successfully",
            pipeline_name=pipeline.get_name(),
            schema_status=schema_status,
            schema_version_hash=schema_version_hash,
            schema_data=schema_data,
        )
    except Exception as e:
        logger.exception(f" Error processing: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"An internal server error occured"}
        )
