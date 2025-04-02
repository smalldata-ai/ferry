import dlt
import urllib.parse
from dlt.common.configuration.specs import AwsCredentials
from ferry.src.destinations.destination_base import DestinationBase

class S3Destination(DestinationBase):

    def default_schema_name(self) -> str:
        return ""

    def dlt_target_system(self, uri: str, **kwargs):  
        """Returns the S3 destination for dlt."""
        bucket_name, aws_credentials = self._parse_s3_uri(uri)
        

        # Extract bucket name and path
        # s3_path = uri.replace("s3://", "")
        # bucket_name, *key_parts = s3_path.split("/")
        # object_key = "/".join(key_parts)  # Remaining path inside the bucket
        return dlt.destinations.filesystem(f"s3://{bucket_name}", aws_credentials)
        
        
    
    def _parse_s3_uri(self, uri: str):
        """Extracts bucket name and AWS credentials from the URI."""
        parsed_uri = urllib.parse.urlparse(uri)
        bucket_name = parsed_uri.hostname
        query_params = urllib.parse.parse_qs(parsed_uri.query)

        aws_credentials = AwsCredentials(
            aws_access_key_id=query_params.get("access_key_id", [None])[0],
            aws_secret_access_key=query_params.get("access_key_secret", [None])[0],
            region_name=query_params.get("region", [None])[0]
        )
        return bucket_name, aws_credentials
