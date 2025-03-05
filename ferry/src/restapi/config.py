import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

class Config:
    """Configuration settings for the application."""
    
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
    CELERY_BACKEND_URL = os.getenv("CELERY_BACKEND_URL")

    # gRPC Configuration
    GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))  # Default to 50051
    GRPC_WORKERS = int(os.getenv("GRPC_WORKERS", 10))  # Default to 10 workers

config = Config()  # Create a global config object
