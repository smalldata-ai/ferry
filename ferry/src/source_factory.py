from urllib.parse import urlparse, parse_qs
from ferry.src.sources.s3_source import S3Source  # Import S3Source

class SourceFactory:
    @staticmethod
    def get(source_uri):
        """Returns the correct source system instance"""
        parsed_uri = urlparse(source_uri)

        if parsed_uri.scheme == "s3":
            # Extract AWS credentials from query parameters
            query_params = parse_qs(parsed_uri.query)
            aws_access_key_id = query_params.get("access_key_id", [None])[0]
            aws_secret_access_key = query_params.get("access_key_secret", [None])[0]
            aws_region = query_params.get("region", [None])[0]
            bucket_name = query_params.get("bucket_name", [None])[0]

            return S3Source(source_uri, aws_access_key_id, aws_secret_access_key, aws_region, bucket_name)

        # Add other source types here...
        raise ValueError(f"Unsupported source type: {source_uri}")
