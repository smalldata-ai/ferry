from celery import Celery
import logging
import traceback
from ferry.src.restapi.pipeline_utils import load_data_endpoint
from ferry.src.restapi.models import LoadDataRequest
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

celery_app = Celery("tasks", broker="redis://localhost:6379", backend="redis://localhost:6379")

@celery_app.task(bind=True, name="ferry.src.tasks.load_data_task")
def load_data_task(self, request_data: dict):
    """Celery task for loading data asynchronously"""
    request = LoadDataRequest(**request_data)  # Convert dict back to Pydantic model
    
    try:
        result = asyncio.run(load_data_endpoint(request))
        return {"status": "success", "result": result}
    except Exception as e:
        logger.exception("❌ Error in load_data_task")

        # ✅ Return a fully serializable error response
        return {
            "status": "error",
            "exc_type": type(e).__name__,  # Include the exception type
            "error_message": str(e),
            "traceback": traceback.format_exc(),  # Include detailed traceback
        }
