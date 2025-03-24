from abc import ABC, abstractmethod
from typing import Any
import hashlib
from dlt.sources.credentials import ConnectionStringCredentials

class SourceBase(ABC):

    @abstractmethod
    def dlt_source_system(self, uri: str, **kwargs) -> Any:
        pass

    def create_credentials(self, uri: str):
        return ConnectionStringCredentials(uri)

    def _pseudonymize_columns(self, row, pseudonymizing_columns):
        """Pseudonymizes specified columns using SHA-256 hashing."""
        salt = "WI@N57%zZrmk#88c"
        for col in pseudonymizing_columns:
            if col in row and row[col] is not None:
                sh = hashlib.sha256()
                sh.update((str(row[col]) + salt).encode())
                row[col] = sh.hexdigest()
        return row
