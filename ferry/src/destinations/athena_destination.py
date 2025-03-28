import dlt
import urllib.parse
from dlt.common.configuration.specs import AwsCredentials
from ferry.src.destinations.destination_base import DestinationBase


class AthenaDestination(DestinationBase):
    def default_schema_name(self):
        return ""

    def dlt_target_system(self, uri: str, **kwargs):
        """Returns the S3 destination for dlt."""
        bucket_name, aws_credentials, work_group = self._parse_athena_uri(uri)
        if not bucket_name.startswith("s3://"):
            bucket_name = f"s3://{bucket_name}"

        return dlt.destinations.athena(
            credentials=aws_credentials, destination_name=bucket_name, athena_work_group=work_group
        )

    def _parse_athena_uri(self, uri: str):
        """Extracts bucket name and AWS credentials from the URI."""
        parsed_uri = urllib.parse.urlparse(uri)
        bucket_name = parsed_uri.hostname
        query_params = urllib.parse.parse_qs(parsed_uri.query)

        aws_credentials = AwsCredentials(
            aws_access_key_id=query_params.get("access_key_id", [None])[0],
            aws_secret_access_key=query_params.get("access_key_secret", [None])[0],
            region_name=query_params.get("region", [None])[0],
        )
        work_group = query_params.get("work_group", [None])[0]
        return bucket_name, aws_credentials, work_group
