# Data Pipeline using DLT

## Overview
This project implements a **data extraction, transformation, and loading (ETL) pipeline** using **DLT (Data Loading Tool)**. It supports transferring data between DuckDB as source and destination**.

## Features
- Uses **DLT** for data extraction and loading
- Implements **FastAPI** for REST API interaction
- Ensures proper URI handling for **DuckDB**, 

## Prerequisites
### Install Dependencies
Make sure you have Python installed and install the required dependencies:
```sh
pip install -r requirements.txt
```

## Running the Application
1. Set the `PYTHONPATH` to ensure correct module resolution:
   ```sh
   set PYTHONPATH=%CD%
   ```

2. Start the FastAPI server using **Uvicorn**:
   ```sh
   uvicorn ferry.src.restapi.app:app --host 0.0.0.0 --port 8001 --reload
   ```

3. The API will be available at:
   ```
   http://127.0.0.1:8001
   ```

## API Usage
### Endpoint: `/load-data`
- **Method:** `POST`
- **Description:** Triggers the ETL pipeline to extract data from the source and load it into the destination.

#### Request Example
```json
{
  "source_uri": "duckdb:///source.duckdb",
  "destination_uri": "duckdb:///destination1.duckdb",
  "source_table_name": "my_table",
  "destination_table_name": "my_output_table",
  "dataset_name": "my_dataset"
}
```

#### Response Example
```json
{
  "status": "success",
  "message": "Data loaded successfully",
  "pipeline_name": "destination1_duckdb",
  "table_processed": "my_output_table"
}
```

## How It Works
1. **Receives API request** with source and destination URIs.
2. **Creates a pipeline** dynamically based on the database type.
3. **Fetches data** from the source using `SourceFactory`.
4. **Loads data** into the destination using `DestinationFactory`.
5. **Returns API response** with pipeline execution details.

## Supported Databases
- **DuckDB** (`duckdb:///file.duckdb`)


## Debugging
- Logs are written using Python's `logging` module.
- To enable debug logs, modify `logger.setLevel(logging.DEBUG)` in `pipeline_utils.py`.

--



