import pytest
from ferry.src.exceptions import InvalidSourceException
from ferry.src.sources.postgres_source import PostgresSource
from ferry.src.source_factory import SourceFactory 




def test_get_postgres_source():
  uri = 'postgres://username:password@localhost/dbname'
  source = SourceFactory.get(uri)
  assert isinstance(source, PostgresSource)


def test_throw_exception_source_not_found():
  uri = 'http://username:password@localhost/dbname'
  with pytest.raises(InvalidSourceException):
    SourceFactory.get(uri)
      
    