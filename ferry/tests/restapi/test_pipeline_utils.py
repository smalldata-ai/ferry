import pytest
import asyncio
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
from ferry.src.restapi.models import LoadDataRequest
from ferry.src.restapi.pipeline_utils import create_pipeline, load_data_endpoint

def test_create_pipeline_duckdb():
    with patch("ferry.src.restapi.pipeline_utils.dlt.pipeline") as mock_pipeline, \
         patch("ferry.src.restapi.pipeline_utils.dlt.destinations.duckdb") as mock_duckdb:
        
        mock_duckdb.return_value = "duckdb_instance"
        mock_pipeline.return_value = MagicMock(destination="duckdb_instance")
        
        pipeline = create_pipeline("test_pipeline", "duckdb:///test_db.duckdb", "test_dataset")
        
        assert pipeline.destination == "duckdb_instance"

def test_load_data_endpoint_success():
    request = LoadDataRequest(
        source_uri="duckdb:///path/to/source_db.duckdb",
        destination_uri="duckdb:///path/to/destination_db.duckdb",
        source_table_name="source_table",
        destination_table_name="destination_table",
        dataset_name="test_dataset"
    )
    
    mock_pipeline = MagicMock()
    mock_pipeline.pipeline_name = "duckdb_pipeline"
    
    with patch("ferry.src.restapi.pipeline_utils.create_pipeline", return_value=mock_pipeline) as mock_create_pipeline, \
         patch("ferry.src.source_factory.SourceFactory.get") as mock_source_factory:
        
        mock_source = MagicMock()
        mock_source_factory.return_value.dlt_source_system.return_value = mock_source
        
        response = asyncio.run(load_data_endpoint(request))
        
        assert response.status == "success"
        assert response.pipeline_name == "duckdb_pipeline"
        assert response.table_processed == "destination_table"

def test_load_data_endpoint_runtime_error():
    request = LoadDataRequest(
        source_uri="duckdb:///invalid_path.duckdb",
        destination_uri="duckdb:///path/to/destination_db.duckdb",
        source_table_name="source_table",
        destination_table_name="destination_table",
        dataset_name="test_dataset"
    )
    
    with patch("ferry.src.restapi.pipeline_utils.create_pipeline", side_effect=RuntimeError("Pipeline creation failed")):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(load_data_endpoint(request))
        
        assert exc_info.value.status_code == 500
        assert "A runtime error occurred" in exc_info.value.detail
