import os
from dotenv import load_dotenv

#  Load .env file
load_dotenv()

class Config:
    """Configuration settings for the application."""
    
    # CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    # CELERY_BACKEND_URL = os.getenv("CELERY_BACKEND_URL", "redis://localhost:6379/0")
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
    CELERY_BACKEND_URL = os.getenv("CELERY_BACKEND_URL")

config = Config()  # Create a global config object
