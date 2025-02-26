from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb
import dlt
import os
import pandas as pd

app = FastAPI()

# Request model for ingestion
class IngestionRequest(BaseModel):
    source_uri: str  # e.g., "duckdb:///path/to/sample_db.duckdb"
    destination_uri: str  # e.g., "duckdb:///path/to/output_db.duckdb"
    source_table_name: str  # e.g., "users"
    destination_table_name: str  # e.g., "users_copy"
    dataset_name: str  # e.g., "test_dataset"


### 🔹 1️⃣ Dynamic Ingestion Endpoint
@app.post("/ingest")
def ingest_data(request: IngestionRequest):
    try:
        # Extract DuckDB file paths from URIs
        source_db = request.source_uri.replace("duckdb:///", "")
        destination_db = request.destination_uri.replace("duckdb:///", "")

        # Check if source DB exists
        if not os.path.exists(source_db):
            raise HTTPException(status_code=404, detail=f"⚠️ Source database '{source_db}' not found!")

        # Connect to source DuckDB
        conn_src = duckdb.connect(source_db)
        
        # Check if the source table exists
        existing_tables = conn_src.execute("SELECT table_name FROM information_schema.tables").fetchall()
        table_names = [t[0] for t in existing_tables]
        
        if request.source_table_name not in table_names:
            conn_src.close()
            raise HTTPException(status_code=404, detail=f"⚠️ Table '{request.source_table_name}' not found in source DB!")

        # Extract data from source table
        df = conn_src.execute(f"SELECT * FROM {request.source_table_name}").fetchdf()
        conn_src.close()

        # Initialize pipeline for data loading
        pipeline = dlt.pipeline(
            pipeline_name="duckdb_pipeline",
            destination=dlt.destinations.duckdb(configuration={"database": destination_db}),
            dataset_name=request.dataset_name
        )

        # Load data into destination table
        pipeline.run(df, table_name=request.destination_table_name)

        return {"message": "✅ Data successfully ingested!", "rows_loaded": len(df)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


### 🔹 2️⃣ List Tables in Destination DB
@app.post("/tables")
def list_tables(request: IngestionRequest):
    try:
        destination_db = request.destination_uri.replace("duckdb:///", "")
        
        if not os.path.exists(destination_db):
            return {"message": f"⚠️ Destination database '{destination_db}' does not exist.", "tables": []}

        conn = duckdb.connect(destination_db)
        tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
        conn.close()
        
        return {"tables": [{"schema": t[0], "name": t[1]} for t in tables]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


### 🔹 3️⃣ Fetch Data from a Table in Destination DB
@app.post("/data")
def get_table_data(request: IngestionRequest):
    try:
        destination_db = request.destination_uri.replace("duckdb:///", "")
        
        if not os.path.exists(destination_db):
            raise HTTPException(status_code=404, detail=f"⚠️ Destination database '{destination_db}' not found!")

        conn = duckdb.connect(destination_db)
        
        # Check if table exists
        result = conn.execute("SELECT table_schema FROM information_schema.tables WHERE table_name = ?", (request.destination_table_name,)).fetchall()
        
        if not result:
            conn.close()
            raise HTTPException(status_code=404, detail=f"⚠️ Table '{request.destination_table_name}' not found in destination DB!")

        schema = result[0][0]  # Get the schema name
        
        # Query data
        df = conn.execute(f"SELECT * FROM {schema}.{request.destination_table_name}").fetchdf()
        conn.close()

        return {"table": request.destination_table_name, "schema": schema, "data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


### 🔹 4️⃣ Check if Destination Database Exists
@app.post("/check_db")
def check_db(request: IngestionRequest):
    destination_db = request.destination_uri.replace("duckdb:///", "")
    
    if os.path.exists(destination_db) and os.path.getsize(destination_db) > 0:
        return {"message": f"✅ Database file '{destination_db}' exists and is not empty."}
    return {"message": f"⚠️ Database file '{destination_db}' does not exist or is empty."}
