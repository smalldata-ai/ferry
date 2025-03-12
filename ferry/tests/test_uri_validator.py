import pytest
from ferry.src.uri_validator import URIValidator

class TestURIValidator:
    @pytest.mark.parametrize("uri", [
        "postgresql://user:password@localhost:5432/dbname",
        "postgresql://admin:admin@127.0.0.1:5432/testdb"
    ])
    def test_valid_postgres_uri(self, uri):
        assert URIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "postgresql://localhost:5432/dbname",  # Missing user & password
        "postgresql://user:password@localhost/dbname",  # Missing port
        "postgresql://user:password@localhost:5432/",  # Missing database name
        "postgresql://localhost:5432/",  # Missing user, password, and database
    ])
    def test_invalid_postgres_uri(self, uri):
        with pytest.raises(ValueError, match="Invalid SQL URI"):
            URIValidator.validate_uri(uri)

    @pytest.mark.parametrize("uri", [
        "duckdb:////path/to/db.duckdb",
        "duckdb:////home/user/database.duckdb"
    ])
    def test_valid_duckdb_uri(self, uri):
        assert URIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "duckdb://",  # No file path
        "duckdb:///",  # Empty file path
        "duckdb://invalid-path"  # Invalid file path format
    ])
    def test_invalid_duckdb_uri(self, uri):
        with pytest.raises(ValueError, match="Invalid file-based URI"):
            URIValidator.validate_uri(uri)

    @pytest.mark.parametrize("uri", [
        "s3://mybucket?access_key_id=aa&access_key_secret=aa&region=cc",        
    ])
    def test_valid_s3_uri(self, uri):
        assert URIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "s3://",  # No bucket name
        "s3://mybucket",  # Missing file_key
        "s3://mybucket?access_key_secret=aa&region=cc"
        "s3://mybucket?access_key_id=aa&region=cc"
        "s3://mybucket?access_key_id=aa&access_key_secret=cc"
    ])
    def test_invalid_s3_uri(self, uri):
        with pytest.raises(ValueError, match="Invalid S3 URI format"):
            URIValidator.validate_uri(uri)


    @pytest.mark.parametrize("uri", [
        "mongodb://user:password@localhost:5432/auth_dbname?database=db_name",        
        "mongodb://user:password@localhost:5432/?database=db_name",
    ])
    def test_valid_mongodb_uri(self, uri):
        assert URIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "mongodb://@localhost:5432/auth_dbname?database=db_name",
        "mongodb://user:password@:5432/auth_dbname?database=db_name",        
        "mongodb://user:password@localhost:/?database=db_name",               
        "mongodb://user:password@localhost:5432/auth_dbname",
    ])
    def test_invalid_mongodb_uri(self, uri):
        with pytest.raises(ValueError, match="Invalid MongoDB URI format"):
            URIValidator.validate_uri(uri)            

    @pytest.mark.parametrize("uri", [
        "snowflake://user:password@account/dbName/dataset",
    ])
    def test_valid_snowflake_uri(self, uri):
        assert URIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "snowflake://:password@account/dbName/dataset",        
        "snowflake://user:@account/dbName/dataset",        
        "snowflake://user:password@/dbName/dataset",
        "snowflake://user:password@account//dataset",
        "snowflake://user:password@account/dbName",
    ])
    def test_invalid_snowflake_uri(self, uri):
        with pytest.raises(ValueError, match="Invalid Snowflake URI format"):
            URIValidator.validate_uri(uri)            


    @pytest.mark.parametrize("uri", [
        "md://ferrytest?token=token",
    ])
    def test_valid_motherduck_uri(self, uri):
        assert URIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "md://?token=token",     
        "md://db_name",     
        
    ])
    def test_invalid_motherduck_uri(self, uri):
        with pytest.raises(ValueError, match="Invalid Motherduck URI format"):
            URIValidator.validate_uri(uri)            

    @pytest.mark.parametrize("uri", [
        "bigquery://project_id?client_id=7-6hty54hur.apps.googleusercontent.com&client_secret=d-12ed3&refresh_token=jksjdk989111",
    ])
    def test_valid_bigquery_uri(self, uri):
        assert URIValidator.validate_uri(uri) == uri

    @pytest.mark.parametrize("uri", [
        "bigquery://?client_id=7-6hty54hur.apps.googleusercontent.com&client_secret=d-12ed3&refresh_token=jksjdk989111",
        "bigquery://project_id?client_id=7-6hty54hur.apps.googleusercontent.com&client_secret=d-12ed3",
        
    ])
    def test_invalid_bigquery_iri(self, uri):
        with pytest.raises(ValueError, match="Invalid Bigquery URI format"):
            URIValidator.validate_uri(uri)            

