from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3
import pandas as pd
import os
from sqlalchemy import create_engine, text

app = FastAPI()

# Request model for ingestion
class IngestionRequest(BaseModel):
    s3_bucket: str  # e.g., "my-bucket"
    s3_key: str  # e.g., "data/sample.csv"
    aws_access_key: str
    aws_secret_key: str
    aws_region: str
    destination_uri: str  # e.g., "postgresql://user:pass@localhost:5432/mydb"
    destination_table_name: str  # e.g., "users"
    dataset_name: str  # e.g., "test_dataset"


### 🔹 1️⃣ Ingest CSV from S3 into DB
@app.post("/ingest")
def ingest_data(request: IngestionRequest):
    try:
        # Initialize S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=request.aws_access_key,
            aws_secret_access_key=request.aws_secret_key,
            region_name=request.aws_region,
        )

        # Check if file exists in S3
        try:
            s3_client.head_object(Bucket=request.s3_bucket, Key=request.s3_key)
        except Exception:
            raise HTTPException(status_code=404, detail=f"⚠️ S3 file '{request.s3_bucket}/{request.s3_key}' not found!")

        # Download file from S3
        file_obj = s3_client.get_object(Bucket=request.s3_bucket, Key=request.s3_key)
        df = pd.read_csv(file_obj["Body"])

        # Ensure non-empty dataframe
        if df.empty:
            raise HTTPException(status_code=400, detail="⚠️ CSV file is empty!")

        # Connect to the database
        engine = create_engine(request.destination_uri)
        with engine.begin() as conn:
            # Load data into the database
            df.to_sql(request.destination_table_name, conn, if_exists="replace", index=False)

        return {"message": "✅ Data successfully ingested!", "rows_loaded": len(df)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


### 🔹 2️⃣ List Tables in Destination DB
@app.post("/tables")
def list_tables(request: IngestionRequest):
    try:
        engine = create_engine(request.destination_uri)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
            tables = [row[0] for row in result.fetchall()]

        return {"tables": tables}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 


### 🔹 3️⃣ Fetch Data from a Table in Destination DB
@app.post("/data")
def get_table_data(request: IngestionRequest):
    try:
        engine = create_engine(request.destination_uri)
        with engine.connect() as conn:
            # Check if the table exists
            result = conn.execute(text(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{request.destination_table_name}')"))
            table_exists = result.scalar()
            
            if not table_exists:
                raise HTTPException(status_code=404, detail=f"⚠️ Table '{request.destination_table_name}' not found!")

            # Fetch data
            df = pd.read_sql(f"SELECT * FROM {request.destination_table_name}", conn)

        return {"table": request.destination_table_name, "data": df.to_dict(orient="records")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 


### 🔹 4️⃣ Check if Destination Database Exists
@app.post("/check_db")
def check_db(request: IngestionRequest):
    try:
        engine = create_engine(request.destination_uri)
        with engine.connect():
            return {"message": f"✅ Database connection successful: {request.destination_uri}"}
    except Exception:
        return {"message": f"⚠️ Unable to connect to database: {request.destination_uri}"}
