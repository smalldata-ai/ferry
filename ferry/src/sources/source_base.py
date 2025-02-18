from abc import ABC, abstractmethod

from dlt.sources.credentials import ConnectionStringCredentials

class SourceBase(ABC):

    @abstractmethod
    def dlt_source_system(self, uri: str, table_name: str, **kwargs):
        pass

    def create_credentials(self, uri: str):
        return ConnectionStringCredentials(uri)