from pydantic import BaseModel

class LoadDataRequest(BaseModel):
    source_uri: str
    destination_uri: str
    dataset_name: str
    source_table_name: str
    destination_table_name: str

class LoadDataResponse(BaseModel):
    status: str
    message: str
    pipeline_name: str
    table_processed: str
