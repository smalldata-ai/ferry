# config.py

import os

# Read from environment variables or use defaults
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

# Construct Redis URLs
CELERY_BROKER_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"
CELERY_BACKEND_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"
