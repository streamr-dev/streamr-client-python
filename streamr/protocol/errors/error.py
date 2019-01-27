
class InvalidJsonError(Exception):

    def __init__(self, streamId, jsonString, parseError, streamMessage):
        super().__init__('Invalid JSON in stream %s: %s. Error while parsing was : %s'%(streamId,jsonString,parseError))
        self.streamId = streamId
        self.jsonString = jsonString
        self.parseError = parseError
        self.streamMessage = streamMessage


class UnsupportedVersionError(Exception):

    def __init__(self,version,message):
        super().__init__('Unsupported version : %s, message %s'%(version,message))
        self.version = version
        self.message = message

