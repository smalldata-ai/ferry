import logging
import os
import dlt
import yaml

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.data_models.response_models import IngestResponse, LoadStatus, SchemaResponse
from ferry.src.pipeline_builder import PipelineBuilder

from ferry.src.pipeline_metrics import PipelineMetrics
from ferry.src.security import SecretsManager
from ferry.main import SECURE_MODE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    error_dict = {}
    for error in exc.errors():
        field = error["loc"][-1]
        message = error["msg"]
        if field not in error_dict:
            error_dict[field] = []
        error_dict[field].append(message)
    return JSONResponse(status_code=422, content={"errors": error_dict})


@app.post("/ferry", response_model=IngestResponse)
def ingest(ingest_model: IngestModel):
    """API endpoint to ferry data from source to destination"""
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
        logger.exception(f"Error processing: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": "An internal server error occurred"},
        )


@app.get("/schema", response_model=SchemaResponse)
def get_schema(pipeline_name: str = Query(..., description="The name of the pipeline")):
    try:
        pipeline = dlt.pipeline(pipeline_name=pipeline_name)
        schema_string = pipeline.default_schema.to_pretty_yaml()
        return SchemaResponse(
            pipeline_name=pipeline_name,
            pipeline_schema=schema_string,
        )
    except Exception as e:
        logger.exception(f"Error fetching schema: {e}")
        return JSONResponse(
            status_code=500, content={"status": "error", "message": "Failed to fetch schema"}
        )


@app.get("/ferry/{identity}/observe")
def observe(identity: str):
    """API endpoint to observe a ferry pipeline"""
    try:
        result = PipelineMetrics(name=identity).generate_metrics()

        if result["status"] == "error" or "not found" in (result.get("error") or "").lower():
            raise HTTPException(status_code=404, detail=f"Pipeline '{identity}' not found")

        return result

    except HTTPException as e:
        raise e  # Reraise explicitly
    except Exception as e:
        logger.exception(f"Error processing observe for '{identity}': {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": "An internal server error occurred"},
        )


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if SECURE_MODE:
        required_headers = ["X-Client-Id", "X-Timestamp", "X-Signature"]
        if not all(h in request.headers for h in required_headers):
            raise HTTPException(400, "Missing authentication headers")
        try:
            body = await request.body()
            SecretsManager.verify_request(
                request.headers["X-Client-Id"],
                request.headers["X-Timestamp"],
                request.headers["X-Signature"],
                body,
            )
        except UnicodeDecodeError:
            raise HTTPException(400, "Malformed request body")
        except HTTPException as e:
            return JSONResponse(status_code=e.status_code, content={"detail": e.detail})
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return JSONResponse(
                status_code=500, content={"detail": "Internal authentication error"}
            )
    response = await call_next(request)
    return response
