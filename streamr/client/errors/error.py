

class ConnectionError(Exception):

    def __init__(self,error):
        super().__init__('Connection Error, message %s' % (error))
        self.error = error
