import pytest
from pydantic import ValidationError
from ferry.src.restapi.database_uri_validator import DatabaseURIValidator

class TestDatabaseURIValidator:
    @pytest.mark.parametrize("uri", [
        "postgresql://user:password@localhost:5432/dbname",
        "postgresql://admin:admin@127.0.0.1:5432/testdb"
    ])
    def test_valid_postgres_uri(self, uri):
        assert DatabaseURIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "postgresql://localhost:5432/dbname",  # Missing user & password
        "postgresql://user:password@localhost/dbname",  # Missing port
        "postgresql://user:password@localhost:5432/",  # Missing database name
        "postgresql://localhost:5432/",  # Missing user, password, and database
    ])
    def test_invalid_postgres_uri(self, uri):
        with pytest.raises(ValueError, match="PostgreSQL URI must"):
            DatabaseURIValidator.validate_uri(uri)

    @pytest.mark.parametrize("uri", [
        "duckdb:///path/to/db.duckdb",
        "duckdb:///home/user/database.duckdb"
    ])
    def test_valid_duckdb_uri(self, uri):
        assert DatabaseURIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "duckdb://",  # No file path
        "duckdb:///",  # Empty file path
        "duckdb://invalid-path"  # Invalid file path format
    ])
    def test_invalid_duckdb_uri(self, uri):
        with pytest.raises(ValueError, match="DuckDB URI must"):
            DatabaseURIValidator.validate_uri(uri)

    @pytest.mark.parametrize("uri", [
        "s3://mybucket?file_key=data.parquet",
        "s3://dataset-bucket?file_key=training_data.csv"
    ])
    def test_valid_s3_uri(self, uri):
        assert DatabaseURIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "s3://",  # No bucket name
        "s3://mybucket",  # Missing file_key
        "s3://mybucket?other_param=value"  # Missing file_key parameter
    ])
    def test_invalid_s3_uri(self, uri):
        with pytest.raises(ValueError, match="S3 URI must"):
            DatabaseURIValidator.validate_uri(uri)

    @pytest.mark.parametrize("value", ["table_name", "dataset"])
    def test_valid_non_empty(self, value):
        assert DatabaseURIValidator.validate_non_empty(value) == value

    @pytest.mark.parametrize("value", ["", None])
    def test_invalid_non_empty(self, value):
        with pytest.raises(ValueError, match="Value must be provided"):
            DatabaseURIValidator.validate_non_empty(value)
