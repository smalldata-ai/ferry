import logging
from http.client import HTTPException
import os
import string
import dlt

from fastapi import FastAPI,Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import yaml

from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.data_models.response_models import IngestResponse, LoadStatus, SchemaResponse
from ferry.src.data_models.schema_request_model import SchemaRequest
from ferry.src.pipeline_builder import PipelineBuilder


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


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

        pipeline_schema_path = os.path.join(".schemas", f"{ingest_model.identity}.schema.yaml")
        schema_version_hash = None

        if os.path.exists(pipeline_schema_path):
            with open(pipeline_schema_path, "r") as f:
                schema_data = yaml.safe_load(f)
            schema_version_hash = schema_data.get("version_hash", "")

        return IngestResponse(
            status=LoadStatus.SUCCESS.value,
            message="Data Ingestion is completed successfully",
            pipeline_name=pipeline.get_name(),
            schema_version_hash=schema_version_hash,
        )
    except Exception as e:
        logger.exception(f" Error processing: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"An internal server error occured"}
        )

@app.get("/schema")
def get_schema(schema_request: SchemaRequest):
    pipeline = dlt.pipeline(pipeline_name=schema_request.pipeline_name)
    schema = pipeline.default_schema.to_dict()

    return SchemaResponse(
        pipeline_name=schema_request.pipeline_name,
        pipeline_schema=schema,
    )