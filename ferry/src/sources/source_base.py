from abc import ABC, abstractmethod
from typing import Any
from dlt.sources.credentials import ConnectionStringCredentials

class SourceBase(ABC):

    @abstractmethod
    def dlt_source_system(self, uri: str, **kwargs) -> Any:
        pass


    def create_credentials(self, uri: str):
        return ConnectionStringCredentials(uri)