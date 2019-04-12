"""
This doc provide Errors
"""

__all__ = ['InvalidJsonError', 'UnsupportedVersionError',
           'UnSupportedPayloadError', 'AbstractFunctionError']


class InvalidJsonError(Exception):
    """
    When msg can't be parsed with json format
    """

    def __init__(self, stream_id, json_string, parse_error, stream_message):
        super().__init__('Invalid JSON in stream %s: %s. Error while parsing was : %s' %
                         (stream_id, json_string, parse_error))
        self.stream_id = stream_id
        self.json_string = json_string
        self.parse_error = parse_error
        self.stream_message = stream_message


class UnsupportedVersionError(Exception):
    """
    when request version is unsupported
    """

    def __init__(self, version, message):
        super().__init__('Unsupported version : %s : %s' % (version, message))
        self.version = version
        self.message = message


class UnSupportedPayloadError(Exception):
    """
    payload is not supported
    """

    def __init__(self, msg):
        super().__init__('UnsupportedPayloadError :' % msg)


class AbstractFunctionError(Exception):
    """
    abstract function  can't be called
    """

    def __init__(self, clazz):
        super().__init__('Abstract function should be override by son class : %s' % clazz)
