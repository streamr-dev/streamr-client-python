"""
This doc provides the request classes
"""


import json

from streamr.util.compare import EqualFunc
from streamr.util.option import Option
from streamr.util.constant import RequestConstant
from streamr.protocol.util.parser import jparser, tparser
from streamr.protocol.util.meta import RequestMeta
from streamr.protocol.errors.error import UnsupportedVersionError


__all__ = ['Request', 'PublishRequest', 'ResendRequest',
           'SubscribeRequest', 'UnsubscribeRequest']


class Request(EqualFunc, metaclass=RequestMeta):
    """
    base class of request
    """

    def __init__(self, request_type, stream_id=None, api_key=None, session_token=None):

        self.request_type = request_type
        self.stream_id = stream_id
        self.api_key = api_key
        self.session_token = session_token

    def to_object(self):
        """
        covert Request to dict
        :return: dict
        """
        return {RequestConstant.TYPE: self.request_type,
                RequestConstant.STREAM_ID: self.stream_id,
                RequestConstant.API_KEY: self.api_key,
                RequestConstant.SESSION_TOKEN: self.session_token}

    def serialize(self):
        """
        convert Request to str
        :return: str
        """
        return json.dumps(self.to_object())

    @classmethod
    def check_version(cls, msg):
        """
        whether the msg version is valid
        :param msg: str object
        :return: bool
        """
        version = msg.get('version', None)
        if version is not None and version != 0:
            raise UnsupportedVersionError(version, 'only version 0 is valid')

    @classmethod
    def deserialize(cls, msg):
        """
        convert msg to Request object
        :param msg: str or dict
        :return: Request object
        """
        msg = jparser(msg)
        cls.check_version(msg)
        request_class = cls.response_class_by_response_type[msg[RequestConstant.TYPE]]
        args = request_class.get_constructor_arguments(msg)
        return request_class(*args)


class PublishRequest(Request):
    """
    Publish request class
    """

    TYPE = 'publish'

    def __init__(self, stream_id, api_key, session_token, content,
                 timestamp=None, partition_key=None,
                 publisher_address=None, signature_type=None,
                 signature=None):
        super().__init__(self.TYPE, stream_id, api_key, session_token)
        if content is None:
            raise ValueError('No content given')

        self.content = content
        self.timestamp = timestamp
        self.partition_key = partition_key
        self.publisher_address = publisher_address
        self.signature_type = signature_type
        self.signature = signature

    def get_timestamp_as_number(self):
        """
        return timestamp as number
        :return: int or float
        """
        if self.timestamp:
            return tparser(self.timestamp)
        return None

    def get_serialized_content(self):
        """
        serialize content
        :return: str
        """
        if isinstance(self.content, str):
            return self.content
        elif isinstance(self.content, (list, dict)):
            return json.dumps(self.content)

        raise ValueError('Stream payload can only be object')

    def to_object(self):
        """
        convert PublishRequest to object
        :return: dict
        """
        return {**super().to_object(),
                **{RequestConstant.SERIALIZED_CONTENT: self.get_serialized_content(),
                   RequestConstant.TIMESTAMP: self.get_timestamp_as_number(),
                   RequestConstant.PARTITION_KEY: self.partition_key,
                   RequestConstant.PUBLISHER_ADDRESS: self.publisher_address,
                   RequestConstant.SIGNATURE_TYPE: self.signature_type,
                   RequestConstant.SIGNATURE: self.signature}}

    @classmethod
    def get_constructor_arguments(cls, msg):
        """
        get arguments from msg
        :param msg: dict
        :return: list
        """
        return [msg.get(RequestConstant.STREAM_ID, None),
                msg.get(RequestConstant.API_KEY, None),
                msg.get(RequestConstant.SESSION_TOKEN, None),
                msg.get(RequestConstant.SERIALIZED_CONTENT, None),
                msg.get(RequestConstant.TIMESTAMP, None),
                msg.get(RequestConstant.PARTITION_KEY, None),
                msg.get(RequestConstant.PUBLISHER_ADDRESS, None),
                msg.get(RequestConstant.SIGNATURE_TYPE, None),
                msg.get(RequestConstant.SIGNATURE, None)]


class ResendRequest(Request):
    """
    Resend Request
    """
    TYPE = 'resend'

    def __init__(self, stream_id, stream_partition=0, sub_id=None,
                 resend_option=None, api_key=None, session_token=None):

        super().__init__(self.TYPE, stream_id, api_key, session_token)

        if not isinstance(resend_option, Option) or \
                (not resend_option.resend_all and not resend_option.resend_from_time):
            raise ValueError('Invalid resend option')

        if not sub_id:
            raise ValueError('Subscription ID not given')

        self.stream_partition = stream_partition
        self.sub_id = sub_id
        self.resend_option = resend_option

    def to_object(self):
        """
        convert ResendRequest to object
        :return: dict
        """
        return {**super().to_object(),
                **{RequestConstant.STREAM_PARTITION: self.stream_partition,
                   RequestConstant.SUB_ID: self.sub_id},
                **self.resend_option.to_resend_object()}

    @classmethod
    def get_constructor_arguments(cls, msg):
        """
        get arguments from msg
        :param msg: dict
        :return: list
        """
        return [msg.get(RequestConstant.STREAM_ID, None),
                msg.get(RequestConstant.STREAM_PARTITION, None),
                msg.get(RequestConstant.SUB_ID, None),
                Option.deserialize_resend(msg),
                msg.get(RequestConstant.API_KEY, None),
                msg.get(RequestConstant.SESSION_TOKEN, None)]


class SubscribeRequest(Request):
    """
    Subscribe Request
    """
    TYPE = 'subscribe'

    def __init__(self, stream_id, stream_partition=0, api_key=None, session_token=None):

        super().__init__(self.TYPE, stream_id, api_key, session_token)

        self.stream_partition = stream_partition

    def to_object(self):
        """
        convert subscribeRequest to dict
        :return: dict
        """
        return {**super().to_object(), RequestConstant.STREAM_PARTITION: self.stream_partition}

    @classmethod
    def get_constructor_arguments(cls, msg):
        """
        get arguments from msg
        :param msg: dict
        :return: list
        """
        return [msg.get(RequestConstant.STREAM_ID, None),
                msg.get(RequestConstant.STREAM_PARTITION, None),
                msg.get(RequestConstant.API_KEY, None),
                msg.get(RequestConstant.SESSION_TOKEN, None)]


class UnsubscribeRequest(Request):
    """
    Unsubscribe Request class
    """
    TYPE = 'unsubscribe'

    def __init__(self, stream_id, stream_partition=0, api_key=None, session_token=None):
        super().__init__(self.TYPE, stream_id, api_key, session_token)

        self.stream_partition = stream_partition

    def to_object(self):
        """
        convert UnsubscribeRequest to dict
        :return: dict
        """
        return {**super().to_object(), RequestConstant.STREAM_PARTITION: self.stream_partition}

    @classmethod
    def get_constructor_arguments(cls, msg):
        """
        get arguments from msg
        :param msg: dict
        :return: list
        """
        return [msg.get(RequestConstant.STREAM_ID, None),
                msg.get(RequestConstant.STREAM_PARTITION, None),
                msg.get(RequestConstant.API_KEY, None),
                msg.get(RequestConstant.SESSION_TOKEN, None)]
