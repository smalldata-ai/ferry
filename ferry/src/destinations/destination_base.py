from abc import ABC, abstractmethod

class DestinationBase(ABC):
    
    
    @abstractmethod
    def dlt_target_system(self, uri: str, **kwargs):
        pass