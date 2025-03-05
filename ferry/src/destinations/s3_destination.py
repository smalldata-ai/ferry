import dlt
from ferry.src.destinations.destination_base import DestinationBase

class S3Destination(DestinationBase):

    def dlt_target_system(self, uri: str, aws_access_key_id: str, aws_secret_access_key: str, **kwargs):  
        """Returns the S3 destination for dlt."""
        
        # Ensure the URI starts with s3://
        if not uri.startswith("s3://"):
            raise ValueError("Invalid S3 URI. It must start with 's3://'.")

        # Extract bucket name and path
        s3_path = uri.replace("s3://", "")
        bucket_name, *key_parts = s3_path.split("/")
        object_key = "/".join(key_parts)  # Remaining path inside the bucket

        if not bucket_name or not object_key:
            raise ValueError("S3 URI must contain both bucket name and file path.")

        # Configure the S3 destination
        return dlt.destinations.s3(
            configuration={
                "bucket": bucket_name,
                "path": object_key,
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
            },
            **kwargs
        )
