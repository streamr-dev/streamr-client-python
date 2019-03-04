
__all__ = ['ConnectionError', 'ParameterError',
           'ConnectionFailedError', 'MessageError']


class ConnectionError(Exception):

    def __init__(self, error):
        super().__init__('Connection Error: %s' % (error))
        self.error = error


class ConnectionFailedError(Exception):
    def __init__(self, msg):
        super().__init__('Connection Failed: %s ' % (msg))


class ParameterError(Exception):

    def __init__(self, msg):
        super().__init__('Parameter Error: %s ' % (msg))


class MessageError(Exception):

    def __init__(self, msg):
        super().__init__('Message Error: cant deserilize this message: %s ' % (msg))
