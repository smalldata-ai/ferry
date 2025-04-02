class InvalidSourceException(Exception):
    def __init__(self, message="Invalid source scheme provided"):
        self.message = message
        super().__init__(self.message)

class InvalidDestinationException(Exception):
    def __init__(self, message="Invalid destination scheme provided"):
        self.message = message
        super().__init__(self.message)