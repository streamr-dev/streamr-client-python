"""
Provide errors used in client module
"""


__all__ = ['ConnectionErr']


class ConnectionErr(Exception):
    """
    Connection Error
    """

    def __init__(self, error):
        super().__init__('Connection Error: %s' % error)
        self.error = error

