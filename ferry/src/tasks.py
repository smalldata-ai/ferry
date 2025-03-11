from celery import Celery
import logging
import traceback
from ferry.src.restapi.pipeline_utils import load_data_endpoint
from ferry.src.restapi.models import LoadDataRequest
import asyncio
from ferry.src.restapi.config import config
from ferry.src.logging.ferry_log_collector import FerryLogCollector  # Fixed import

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use values from config.py instead of hardcoded URLs
celery_app = Celery("tasks", broker=config.CELERY_BROKER_URL, backend=config.CELERY_BACKEND_URL)

@celery_app.task(bind=True, name="ferry.src.tasks.load_data_task")
def load_data_task(self, request_data: dict):
    """Celery task for loading data asynchronously"""
    request = LoadDataRequest(**request_data)  # Convert dict back to Pydantic model
    task_id = self.request.id  # Extract Celery task ID

    try:
        result = asyncio.run(load_data_endpoint(request, task_id))  # ✅ Pass task_id
        return {"status": "success", "result": result}
    except Exception as e:
        logger.exception("❌ Error in load_data_task")

        return {
            "status": "error",
            "exc_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc(),
        }
