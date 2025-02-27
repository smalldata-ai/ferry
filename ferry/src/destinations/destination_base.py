from abc import ABC, abstractmethod

class DestinationBase(ABC):
    
    @abstractmethod
    def dlt_destination_name(self, uri: str, table_name: str, **kwargs):
        pass

    @abstractmethod
    def dlt_target_system(self, uri: str, **kwargs):
        pass