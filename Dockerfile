# Use an official Python image as a base
FROM python:3.10-slim

# Set environment variables
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install system dependencies required for data movement
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libsqlite3-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create and activate a virtual environment
RUN python -m venv $VIRTUAL_ENV

# Set the working directory inside the container
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt . 
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the entire ferry project
COPY . .            

# Add `/app` to Python's module search path
ENV PYTHONPATH="/app"

# Expose the application port
EXPOSE 8001

# Change entrypoint to correctly locate main.py
CMD ["python", "ferry/main.py", "serve"]
