from abc import ABC, abstractmethod


class DestinationBase(ABC):
    @abstractmethod
    def default_schema_name(self):
        pass

    @abstractmethod
    def dlt_target_system(self, uri: str, **kwargs):
        pass
