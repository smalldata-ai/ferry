from pydantic import BaseModel, Field, field_validator

class LoadDataRequest(BaseModel):

    source_uri: str = Field('', description="Source credentials as a connection string uri")
    destination_uri: str = Field('', description="Destination credentials as a connection string uri")
    source_table_name: str = Field('', description="Table to load from")
    destination_table_name: str = Field('', description="Table to load from")
    dataset_name: str = Field('', description="Dataset to which the data will be loaded.")

    @field_validator('source_uri')
    def validate_source_uri(v):
        if not v:
            raise ValueError("Source uri must be provided")
        return v
    
    @field_validator('destination_uri')
    def validate_destination_uri(v):
        if not v:
            raise ValueError("Destination uri must be provided")
        return v

    @field_validator('source_table_name')
    def validate_source_table_name(v):
        if not v:
            raise ValueError("Source table name must be provided")
        return v
    
    @field_validator('destination_table_name')
    def validate_destination_table_name(v):
        if not v:
            raise ValueError("Destination table name must be provided")
        return v
    
    @field_validator('dataset_name')
    def validate_dataset_name(v):
        if not v:
            raise ValueError("Dataset name must be provided")
        return v

class LoadDataResponse(BaseModel):
    status: str
    message: str
    pipeline_name: str
    table_processed: str