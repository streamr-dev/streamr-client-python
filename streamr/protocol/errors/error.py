
class InvalidJsonError(Exception):

    def __init__(self, streamId, jsonString, parseError, streamMessage):
        super().__init__('Invalid JSON in stream %s: %s. Error while parsing was : %s' %
                         (streamId, jsonString, parseError))
        self.streamId = streamId
        self.jsonString = jsonString
        self.parseError = parseError
        self.streamMessage = streamMessage


class UnsupportedVersionError(Exception):

    def __init__(self, version, message):
        super().__init__('Unsupported version : %s : %s' % (version, message))
        self.version = version
        self.message = message


class UnSupportedMsgTypeError(Exception):

    def __init__(self, msgType):
        super().__init__('Unknown Message format : %s ' % (msgType))


class UnSupportedPayloadError(Exception):

    def __init__(self, msg):
        super().__init__('UnsupportedPayloadError :' % (msg))


class ParameterError(Exception):

    def __init__(self, msg):
        super().__init__('Parameter Error :  %s ' % (msg))


class AbstractFunctionError(Exception):

    def __init__(self, clazz):
        super().__init__('Absctract function should be overrided by son class : %s' % (clazz))
