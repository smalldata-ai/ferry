import pytest
import asyncio
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
from ferry.src.restapi.models import LoadDataRequest
from ferry.src.restapi.pipeline_utils import create_pipeline, load_data_endpoint

def test_create_pipeline_s3():
    """Test pipeline creation with S3 as source and ClickHouse as destination."""
    with patch("ferry.src.restapi.pipeline_utils.dlt.pipeline") as mock_pipeline, \
         patch("ferry.src.restapi.pipeline_utils.dlt.destinations.clickhouse") as mock_clickhouse:
        
        mock_clickhouse.return_value = "clickhouse_instance"
        mock_pipeline.return_value = MagicMock(destination="clickhouse_instance")
        
        pipeline = create_pipeline("test_pipeline", "clickhouse://user:password@localhost:9000/mydb", "test_dataset")
        
        assert pipeline.destination == "clickhouse_instance"

@pytest.mark.asyncio
async def test_load_data_endpoint_success_s3():
    """Test load_data_endpoint when loading from S3 works correctly."""
    request = LoadDataRequest(
        source_uri="s3://my-bucket/data.csv",
        destination_uri="clickhouse://user:password@localhost:9000/mydb",
        source_table_name="s3_table",
        destination_table_name="destination_table",
        dataset_name="test_dataset"
    )

    mock_pipeline = MagicMock()
    mock_pipeline.pipeline_name = "s3_pipeline"

    with patch("ferry.src.restapi.pipeline_utils.create_pipeline", return_value=mock_pipeline) as mock_create_pipeline, \
         patch("ferry.src.source_factory.SourceFactory.get") as mock_source_factory, \
         patch("boto3.client") as mock_boto3_client:
        
        # Mock S3 client interaction
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=b"sample,csv,data"))
        }

        # Mock Source Factory
        mock_source = MagicMock()
        mock_source_factory.return_value.dlt_source_system.return_value = mock_source

        response = await load_data_endpoint(request)

        assert response.status == "success"
        assert response.pipeline_name == "s3_pipeline"
        assert response.table_processed == "destination_table"

@pytest.mark.asyncio
async def test_load_data_endpoint_s3_runtime_error():
    """Test load_data_endpoint when pipeline creation fails for S3."""
    request = LoadDataRequest(
        source_uri="s3://invalid-bucket/data.csv",
        destination_uri="clickhouse://user:password@localhost:9000/mydb",
        source_table_name="s3_table",
        destination_table_name="destination_table",
        dataset_name="test_dataset"
    )

    with patch("ferry.src.restapi.pipeline_utils.create_pipeline", side_effect=RuntimeError("Pipeline creation failed")):
        with pytest.raises(HTTPException) as exc_info:
            await load_data_endpoint(request)

        assert exc_info.value.status_code == 500
        assert "A runtime error occurred" in exc_info.value.detail

@pytest.mark.asyncio
async def test_load_data_endpoint_s3_invalid_bucket():
    """Test load_data_endpoint when S3 bucket does not exist."""
    request = LoadDataRequest(
        source_uri="s3://non-existent-bucket/data.csv",
        destination_uri="clickhouse://user:password@localhost:9000/mydb",
        source_table_name="s3_table",
        destination_table_name="destination_table",
        dataset_name="test_dataset"
    )

    with patch("boto3.client") as mock_boto3_client:
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_s3_client.get_object.side_effect = Exception("NoSuchBucket: The specified bucket does not exist")

        with pytest.raises(HTTPException) as exc_info:
            await load_data_endpoint(request)

        assert exc_info.value.status_code == 400
        assert "Failed to read from S3" in exc_info.value.detail
