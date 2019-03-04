import json
from streamr.protocol.utils.meta import ResponseMeta
from streamr.protocol.utils.parser import JParser
from streamr.protocol.payloads import StreamMessage, StreamAndPartition, ResendResponsePayload, ErrorPayload
from streamr.protocol.errors.error import UnSupportedPayloadError, AbstractFunctionError, UnsupportedVersionError

__all__ = ['Response', 'BroadcastMessage', 'ErrorResponse',
           'ResendResponseNoResend', 'ResendResponseResending',
           'ResendResponseResent', 'SubscribeResponse',
           'UnicastMessage', 'UnsubscribeResponse']


class Response(metaclass=ResponseMeta):

    def __init__(self, messageType, payload=None, subId=None):
        self.messageType = messageType
        self.payload = payload
        self.subId = subId

        messageClass = self.__class__.messageClassByMessageType.get(
            self.messageType, None)
        if not isinstance(payload, messageClass.getPayloadClass()):
            raise UnSupportedPayloadError('An unexpected payload was passed to %s! Expected : %s, was %s' % (
                messageClass.getMessageName(), type(messageClass.getPayloadClass()), type(payload)))

    @classmethod
    def getPayloadClass(cls):
        raise AbstractFunctionError(type(cls))

    @classmethod
    def getMessageName(cls):
        raise AbstractFunctionError(type(cls))

    def toObject(self, version=0, payloadVersion=28):
        if version == 0:
            return [version, self.messageType, self.subId, self.payload.toObject(payloadVersion)]
        else:
            raise UnsupportedVersionError(version, 'Supported versions: [0]')

    def serialize(self, version=0, payloadVersion=28):
        return json.dumps(self.toObject(version, payloadVersion))

    @classmethod
    def checkVersion(cls, msg):
        version = msg[0]
        if version != 0:
            raise UnsupportedVersionError(version, 'Supported versions: [0]')

    # [version,responsetype,,payload]
    @classmethod
    def deserialize(cls, msg):
        msg = JParser(msg)
        cls.checkVersion(msg)
        payload = cls.messageClassByMessageType[msg[1]].getPayloadClass(
        ).deserialize(msg[3])
        args = cls.messageClassByMessageType[msg[1]].getConstructorArguments(
            msg, payload)
        return cls.messageClassByMessageType[msg[1]](*args)

    def __eq__(self, another):
        if isinstance(another, type(self)):
            return self.messageType == another.messageType and self.payload == another.payload and self.subId == another.subId
        else:
            return False


class BroadcastMessage(Response):

    TYPE = 0

    def __init__(self, msg):
        super().__init__(self.TYPE, msg)

    @classmethod
    def getMessageName(cls):
        return 'BroadcastMessage'

    @classmethod
    def getPayloadClass(cls):
        return StreamMessage

    @classmethod
    def getConstructorArguments(cls, msg, payload):
        return [payload]


class ErrorResponse(Response):
    TYPE = 7

    def __init__(self, msg):
        super().__init__(self.TYPE, msg)

    @classmethod
    def getMessageName(cls):
        return 'ErrorResponse'

    @classmethod
    def getPayloadClass(cls):
        return ErrorPayload

    @classmethod
    def getConstructorArguments(cls, msg, payload):
        return [payload]


class ResendResponse(Response):

    def __init__(self, TYPE, streamId, streamPartition, subId):
        super().__init__(TYPE, ResendResponsePayload(streamId, streamPartition, subId))

    @classmethod
    def getPayloadClass(cls):
        return ResendResponsePayload

    @classmethod
    def getConstructorArguments(cls, msg, payload):
        return [payload.streamId, payload.streamPartition, payload.subId]


class ResendResponseNoResend(ResendResponse):

    TYPE = 6

    def __init__(self, streamId, streamPartition, subId):
        super().__init__(self.TYPE, streamId, streamPartition, subId)

    @classmethod
    def getMessageName(cls):
        return 'ResendResponseNoResend'


class ResendResponseResending(ResendResponse):

    TYPE = 4

    def __init__(self, streamId, streamPartition, subId):
        super().__init__(self.TYPE, streamId, streamPartition, subId)

    @classmethod
    def getMessageName(cls):
        return 'ResendResponseResending'


class ResendResponseResent(ResendResponse):

    TYPE = 5

    def __init__(self, streamId, streamPartition, subId):
        super().__init__(self.TYPE, streamId, streamPartition, subId)

    @classmethod
    def getMessageName(cls):
        return 'ResendResponseResent'


class SubscribeResponse(Response):

    TYPE = 2

    def __init__(self, streamId, streamPartition=0):
        super().__init__(self.TYPE, StreamAndPartition(streamId, streamPartition))

    @classmethod
    def getMessageName(cls):
        return 'SubscribeResponse'

    @classmethod
    def getPayloadClass(cls):
        return StreamAndPartition

    @classmethod
    def getConstructorArguments(cls, msg, payload):
        return [payload.streamId, payload.streamPartition]


class UnicastMessage(Response):

    TYPE = 1

    def __init__(self, msg, subId):
        super().__init__(self.TYPE, msg, subId)

    @classmethod
    def getMessageName(cls):
        return 'UnicastMessage'

    @classmethod
    def getPayloadClass(cls):
        return StreamMessage

    @classmethod
    def getConstructorArguments(cls, msg, payload):
        return [payload, msg[2]]


class UnsubscribeResponse(Response):

    TYPE = 3

    def __init__(self, streamId, streamPartition=0):
        super().__init__(self.TYPE, StreamAndPartition(streamId, streamPartition))

    @classmethod
    def getMessageName(cls):
        return 'UnsubscribeResponse'

    @classmethod
    def getPayloadClass(cls):
        return StreamAndPartition

    @classmethod
    def getConstructorArguments(cls, msg, payload):
        return [payload.streamId, payload.streamPartition]
