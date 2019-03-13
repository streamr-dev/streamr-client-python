"""
This doc provides the response classes
"""

import json

from streamr.util.compare import EqualFunc
from streamr.protocol.payload import StreamMessage, StreamAndPartition, ResendResponsePayload, ErrorPayload
from streamr.protocol.util.meta import ResponseMeta
from streamr.protocol.util.parser import jparser
from streamr.protocol.errors.error import UnSupportedPayloadError, AbstractFunctionError, UnsupportedVersionError


__all__ = ['Response', 'BroadcastMessage', 'ErrorResponse',
           'ResendResponseNoResend', 'ResendResponseResending',
           'ResendResponseResent', 'SubscribeResponse',
           'UnicastMessage', 'UnsubscribeResponse']


class Response(EqualFunc, metaclass=ResponseMeta):
    """
    Response class
    """

    def __init__(self, response_type, payload=None, sub_id=None):
        self.response_type = response_type
        self.payload = payload
        self.sub_id = sub_id

        response_class = self.__class__.response_class_by_response_type[self.response_type]
        if not isinstance(payload, response_class.get_payload_class()):
            raise UnSupportedPayloadError('Response %s expected payload type: %s. %s given' % (
                response_class.get_response_name(), type(response_class.get_payload_class()), type(payload)))

    @classmethod
    def get_payload_class(cls):
        """
        abstract method return payload class
        :return: payload class
        """
        raise AbstractFunctionError(type(cls))

    @classmethod
    def get_response_name(cls):
        """
        abstract method return response name
        :return: response name
        """
        raise AbstractFunctionError(type(cls))

    def to_object(self, version=0, payload_version=28):
        """
        convert Response to dict
        :param version: 0 by default
        :param payload_version: 28 or 29
        :return: dict
        """
        if version == 0:
            return [version, self.response_type, self.sub_id, self.payload.to_object(payload_version)]
        else:
            raise UnsupportedVersionError(version, 'Supported versions: [0]')

    def serialize(self, version=0, payload_version=28):
        """
        serialize Response object
        :param version: 0 by default
        :param payload_version: 28 or 29
        :return: str
        """
        return json.dumps(self.to_object(version, payload_version))

    @classmethod
    def check_version(cls, version):
        """
        check_api request version
        :param version:
        :return:
        """
        if version != 0:
            raise UnsupportedVersionError(version, 'Supported versions: [0]')

    # [version, response_type, sub_id, payload]
    @classmethod
    def deserialize(cls, ori_msg):
        """
        deserialize from msg to response object
        :param ori_msg: str or dict
        :return: response object
        """
        version, response_type, sub_id, payload_msg = jparser(ori_msg)
        cls.check_version(version)
        response_class = cls.response_class_by_response_type[response_type]
        payload = response_class.get_payload_class().deserialize(payload_msg)
        args = response_class.get_constructor_arguments(sub_id, payload)
        return response_class(*args)


class BroadcastMessage(Response):
    """
    BroadcastMessage response class
    """
    TYPE = 0

    def __init__(self, payload):
        super().__init__(self.TYPE, payload)

    @classmethod
    def get_response_name(cls):
        """
        return response name
        :return: str
        """
        return 'BroadcastMessage'

    @classmethod
    def get_payload_class(cls):
        """
        return payload class
        :return: payload class
        """
        return StreamMessage

    @classmethod
    def get_constructor_arguments(cls, _, payload):
        """
        return constructor args
        :param _: sub_id which is useless for broadcast message
        :param payload: payload object
        :return: args
        """
        return [payload]


class ErrorResponse(Response):
    """
    Error response class
    """
    TYPE = 7

    def __init__(self, payload):
        super().__init__(self.TYPE, payload)

    @classmethod
    def get_response_name(cls):
        """
        return response name
        :return: str
        """
        return 'ErrorResponse'

    @classmethod
    def get_payload_class(cls):
        """
        return payload class
        :return: payload class
        """
        return ErrorPayload

    @classmethod
    def get_constructor_arguments(cls, _, payload):
        """
        return constructor args
        :param _: sub_id which is useless for broadcast message
        :param payload: payload object
        :return: args
        """
        return [payload]


class ResendResponse(Response):
    """
    ResendResponse class
    """

    def __init__(self, response_type, stream_id, stream_partition, sub_id):
        super().__init__(response_type, ResendResponsePayload(stream_id, stream_partition, sub_id))

    @classmethod
    def get_payload_class(cls):
        """
        return payload class
        :return: payload class
        """
        return ResendResponsePayload

    @classmethod
    def get_constructor_arguments(cls, _, payload):
        """
        return constructor args
        :param _: sub_id
        :param payload:
        :return:
        """
        return [payload.stream_id, payload.stream_partition, payload.sub_id]


class ResendResponseNoResend(ResendResponse):
    """
    ResendResponseNoResend class
    """
    TYPE = 6

    def __init__(self, stream_id, stream_partition, sub_id):
        super().__init__(self.TYPE, stream_id, stream_partition, sub_id)

    @classmethod
    def get_response_name(cls):
        """
        return response name
        :return: response name
        """
        return 'ResendResponseNoResend'


class ResendResponseResending(ResendResponse):
    """
    ResendResponseResending class
    """
    TYPE = 4

    def __init__(self, stream_id, stream_partition, sub_id):
        super().__init__(self.TYPE, stream_id, stream_partition, sub_id)

    @classmethod
    def get_response_name(cls):
        """
        return response name
        :return: response name
        """
        return 'ResendResponseResending'


class ResendResponseResent(ResendResponse):
    """
    ResendResponseResent class
    """
    TYPE = 5

    def __init__(self, stream_id, stream_partition, sub_id):
        super().__init__(self.TYPE, stream_id, stream_partition, sub_id)

    @classmethod
    def get_response_name(cls):
        """
        return response name
        :return: response name
        """
        return 'ResendResponseResent'


class SubscribeResponse(Response):
    """
    SubscribeResponse class
    """
    TYPE = 2

    def __init__(self, stream_id, stream_partition=0):
        super().__init__(self.TYPE, StreamAndPartition(stream_id, stream_partition))

    @classmethod
    def get_response_name(cls):
        """
        resend response name
        :return: response name
        """
        return 'SubscribeResponse'

    @classmethod
    def get_payload_class(cls):
        """
        return payload class
        :return: payload class
        """
        return StreamAndPartition

    @classmethod
    def get_constructor_arguments(cls, _, payload):
        """
        return constructor args
        :param _: sub_id
        :param payload: payload
        :return: list
        """
        return [payload.stream_id, payload.stream_partition]


class UnicastMessage(Response):
    """
    UnicastMessage class
    """
    TYPE = 1

    def __init__(self, payload, sub_id):
        super().__init__(self.TYPE, payload, sub_id)

    @classmethod
    def get_response_name(cls):
        """
        return response name
        :return: response name
        """
        return 'UnicastMessage'

    @classmethod
    def get_payload_class(cls):
        """
        return payload class
        :return: payload class
        """
        return StreamMessage

    @classmethod
    def get_constructor_arguments(cls, sub_id, payload):
        """
        return constructor args
        :param sub_id: sub_id
        :param payload: payload
        :return: list
        """
        return [payload, sub_id]


class UnsubscribeResponse(Response):
    """
    UnsubscribeResponse class
    """
    TYPE = 3

    def __init__(self, stream_id, stream_partition=0):
        super().__init__(self.TYPE, StreamAndPartition(stream_id, stream_partition))

    @classmethod
    def get_response_name(cls):
        """
        return response name
        :return: response name
        """
        return 'UnsubscribeResponse'

    @classmethod
    def get_payload_class(cls):
        """
        return payload class
        :return:  payload class
        """
        return StreamAndPartition

    @classmethod
    def get_constructor_arguments(cls, _, payload):
        """
        return constructor args
        :param _: sub_id
        :param payload: payload
        :return: list
        """
        return [payload.stream_id, payload.stream_partition]
