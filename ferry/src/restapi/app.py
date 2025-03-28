import logging
import os
import dlt
import yaml

from fastapi import FastAPI, Request, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.data_models.response_models import IngestResponse, LoadStatus, SchemaResponse
from ferry.src.pipeline_builder import PipelineBuilder
from dlt.common.pipeline import ExtractInfo, NormalizeInfo, LoadInfo

from ferry.src.pipeline_metrics import PipelineMetrics

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
            content={"status": "error", "message": "An internal server error occurred"}
        )

@app.get("/schema", response_model=SchemaResponse)
def get_schema(pipeline_name: str = Query(..., description="The name of the pipeline")):
    """Fetch schema for a given pipeline"""
    try:
        pipeline = dlt.pipeline(pipeline_name=pipeline_name)

        # If schema is missing, initialize it dynamically
        if not pipeline.default_schema:
            logger.warning(f"Schema for {pipeline_name} not found. Initializing a new schema.")
            pipeline._initialize_storage()
            return JSONResponse(
                status_code=404,
                content={"status": "error", "message": f"Schema for {pipeline_name} was missing and has been initialized. Run ingestion first."}
            )

        schema_string = pipeline.default_schema.to_pretty_yaml()
        return SchemaResponse(
            pipeline_name=pipeline_name,
            pipeline_schema=schema_string,
        )
    except Exception as e:
        logger.exception(f"Error fetching schema: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": "Failed to fetch schema"})

@app.get("/schemas")
def get_all_schemas():
    """Fetch all available schemas dynamically"""
    try:
        schemas_path = os.path.expanduser("~/.dlt/pipelines")
        available_schemas = {}

        if not os.path.exists(schemas_path):
            return JSONResponse(
                status_code=404,
                content={"status": "error", "message": "No schemas found. Run ingestion first."}
            )

        for pipeline_name in os.listdir(schemas_path):
            schema_dir = os.path.join(schemas_path, pipeline_name, "schemas")
            if os.path.isdir(schema_dir):
                schemas = os.listdir(schema_dir)
                available_schemas[pipeline_name] = schemas

        return {"available_schemas": available_schemas}
    except Exception as e:
        logger.exception(f"Error fetching all schemas: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": "Failed to fetch schemas"})

@app.get("/ferry/{identity}/observe")
def observe(identity: str):
    """API endpoint to observe a ferry pipeline"""
    try:
        result = PipelineMetrics(name=identity).generate_metrics()
        return result
    except Exception as e:
        logger.exception(f"Error processing: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": "An internal server error occurred"}
        )




# import logging
# import os
# import dlt
# import yaml

# from fastapi import FastAPI, Request, Query
# from fastapi import FastAPI,Request
# from fastapi.exceptions import RequestValidationError
# from fastapi.responses import JSONResponse
# from pydantic import BaseModel

# from ferry.src.data_models.ingest_model import IngestModel
# from ferry.src.data_models.response_models import IngestResponse, LoadStatus, SchemaResponse
# from ferry.src.pipeline_builder import PipelineBuilder
# from dlt.common.pipeline import ExtractInfo, NormalizeInfo, LoadInfo

# from ferry.src.pipeline_metrics import PipelineMetrics

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# app = FastAPI()

# @app.exception_handler(RequestValidationError)
# async def validation_exception_handler(request: Request, exc: RequestValidationError):
#     error_dict = {}
#     for error in exc.errors():
#         field = error['loc'][-1]
#         message = error['msg']
#         if field not in error_dict:
#             error_dict[field] = []
#         error_dict[field].append(message)
#     return JSONResponse(
#         status_code=422,
#         content={"errors": error_dict}
#     )

# @app.post("/ferry", response_model=IngestResponse)
# def ingest(ingest_model: IngestModel):
#     """API endpoint to ferry data from source to destination"""
#     try:
#         pipeline = PipelineBuilder(model=ingest_model).build()
#         pipeline.run()

#         pipeline_schema_path = os.path.join(".schemas", f"{ingest_model.identity}.schema.yaml")
#         schema_version_hash = None

#         if os.path.exists(pipeline_schema_path):
#             with open(pipeline_schema_path, "r") as f:
#                 schema_data = yaml.safe_load(f)
#             schema_version_hash = schema_data.get("version_hash", "")

#         return IngestResponse(
#             status=LoadStatus.SUCCESS.value,
#             message="Data Ingestion is completed successfully",
#             pipeline_name=pipeline.get_name(),
#             schema_version_hash=schema_version_hash,
#         )
#     except Exception as e:
#         logger.exception(f"Error processing: {e}")
#         return JSONResponse(
#             status_code=500,
#             content={"status": "error", "message": "An internal server error occurred"}
#         )

# @app.get("/schema", response_model=SchemaResponse)
# def get_schema(pipeline_name: str = Query(..., description="The name of the pipeline")):
#     try:
#         pipeline = dlt.pipeline(pipeline_name=pipeline_name)
#         schema_string = pipeline.default_schema.to_pretty_yaml()
#         return SchemaResponse(
#             pipeline_name= pipeline_name,
#             pipeline_schema= schema_string,
#         )
#     except Exception as e:
#         logger.exception(f"Error fetching schema: {e}")
        # return JSONResponse(status_code=500, content={"status": "error", "message": "Failed to fetch schema"})
    
# @app.get("/ferry/{identity}/observe")
# def observe(identity: str):
#     """API endpoint to observe a ferry pipeline"""
#     try:
#         result = PipelineMetrics(name=identity).generate_metrics()
#         return result
#     except Exception as e:
#         logger.exception(f" Error processing: {e}")
#         return JSONResponse(
#             status_code=500,
#             content={"status": "error", "message": f"An internal server error occured"}
#         )    
