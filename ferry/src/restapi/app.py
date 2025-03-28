import logging
from fastapi import FastAPI,Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ferry.src.data_models.ingest_model import IngestModel
from ferry.src.data_models.response_models import IngestResponse, LoadStatus
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
        return IngestResponse(
            status=LoadStatus.SUCCESS.value,
            message="Data Ingestion is completed successfully",
            pipeline_name=pipeline.get_name(),
        )
    except Exception as e:
        logger.exception(f" Error processing: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"An internal server error occured"}
        )
    
@app.get("/ferry/{identity}/observe")
def observe(identity: str):
    """API endpoint to observe a ferry pipeline"""
    try:
        result = PipelineMetrics(name=identity).generate_metrics()
        return result
    except Exception as e:
        logger.exception(f" Error processing: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"An internal server error occured"}
        )    