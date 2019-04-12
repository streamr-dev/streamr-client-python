"""
This doc provides the payload classes
"""

import json
from streamr.util.compare import EqualFunc
from streamr.protocol.util.parser import jparser
from streamr.protocol.errors.error import InvalidJsonError, UnsupportedVersionError
from streamr.util.constant import StreamMessageConstant, \
    StreamAndPartitionConstant, ResendResponsePayloadConstant, \
    ErrorPayloadConstant

__all__ = ['StreamMessage', 'StreamAndPartition',
           'ResendResponsePayload', 'ErrorPayload']


class StreamMessage(EqualFunc):
    """
    StreamMessage class
    """

    class ContentType:
        """
        store the constant value
        """
        JSON = 27

    def __init__(self, stream_id, stream_partition, timestamp,
                 ttl, offset, previous_offset, content_type,
                 content, signature_type=None,
                 publisher_address=None, signature=None):
        """
        init function
        :param stream_id: string
        :param stream_partition: int
        :param timestamp: int or float
        :param ttl: int
        :param offset: int
        :param previous_offset: int
        :param content_type: StreamMessage.ContentType.JSON
        :param content: string or dict
        :param signature_type:
        :param publisher_address:
        :param signature:
        """
        self.stream_id = stream_id
        self.stream_partition = stream_partition
        self.timestamp = timestamp
        self.ttl = ttl
        self.offset = offset
        self.previous_offset = previous_offset
        self.content_type = content_type
        self.content = content
        self.signature_type = signature_type
        self.publisher_address = publisher_address
        self.signature = signature
        self.parsed_content = None

    def get_parsed_content(self):
        """
        parse the content
        :return dict
        """
        if self.parsed_content is not None:
            pass
        elif self.content_type == StreamMessage.ContentType.JSON and isinstance(self.content, (list, dict)):
            self.parsed_content = self.content
        elif self.content_type == StreamMessage.ContentType.JSON and isinstance(self.content, str):
            try:
                self.parsed_content = json.loads(self.content)
            except json.JSONDecodeError as e:
                raise InvalidJsonError(self.stream_id, self.content, e, self)
        else:
            raise ValueError('content type: %s cannot be parsed.' % type(self.content_type))
        return self.parsed_content

    def get_serialized_content(self):
        """
        serialize content
        :return:str
        """
        if isinstance(self.content, str):
            return self.content
        elif self.content_type == StreamMessage.ContentType.JSON and isinstance(self.content, (list, dict)):
            return json.dumps(self.content)
        else:
            raise ValueError('content type %s cannot be serialized' % type(self.content_type))

    def to_object(self, version=28, parsed_content=False, compact=True):
        """
        convert StreamMessage to dict
        :param version: 28 or 29
        :param parsed_content: whether parse content or not
        :param compact: return a list or a dict
        :return: list or dict
        """
        if version == 28 or version == 29:
            if compact:
                arr = [version, self.stream_id, self.stream_partition,
                       self.timestamp, self.ttl, self.offset,
                       self.previous_offset, self.content_type,
                       self.get_parsed_content() if parsed_content else self.get_serialized_content()]
                if version == 29:
                    arr.append(self.signature_type)
                    arr.append(self.publisher_address)
                    arr.append(self.signature)
            else:
                arr = {StreamMessageConstant.STREAM_ID: self.stream_id,
                       StreamMessageConstant.STREAM_PARTITION: self.stream_partition,
                       StreamMessageConstant.TIMESTAMP: self.timestamp,
                       StreamMessageConstant.TTL: self.ttl,
                       StreamMessageConstant.OFFSET: self.offset,
                       StreamMessageConstant.PREVIOUS_OFFSET: self.previous_offset,
                       StreamMessageConstant.CONTENT_TYPE: self.content_type,
                       StreamMessageConstant.CONTENT: self.get_parsed_content() if parsed_content
                       else self.get_serialized_content()}
                if version == 29:
                    arr[StreamMessageConstant.SIGNATURE_TYPE] = self.signature_type
                    arr[StreamMessageConstant.PUBLISHER_ADDRESS] = self.publisher_address
                    arr[StreamMessageConstant.SIGNATURE] = self.signature
            return arr
        else:
            raise UnsupportedVersionError(
                version, 'Supported version:[ 28, 29 ]')

    def serialize(self, version=28):
        """
        serialize StreamMessage
        :param version: 28 or 29
        :return: str
        """
        return json.dumps(self.to_object(version))

    @classmethod
    def deserialize(cls, ori_msg, parse_content=True):
        """
        convert a str or dict to a StreamMessage object
        :param ori_msg:  str or list
        :param parse_content: whether parse content or not
        :return: StreamMessage object
        """

        msg = jparser(ori_msg)

        # Version 28: [version, stream_id, stream_partition, timestamp,
        #  ttl, offset, previous_offset, content_type, content]

        # Version 29: [version, stream_id, stream_partition, timestamp,
        # ttl,offset, previous_offset, content_type, content,
        # signature_type, address, signature]

        if msg[0] == 28 or msg[0] == 29:
            result = cls(*msg[1:])
            if parse_content:
                result.get_parsed_content()
            return result
        else:
            raise UnsupportedVersionError(msg[0], 'Supported version: [ 28, 29]')

    def is_bye_message(self):
        """
        test whether the message is a BYE message
        :return: bool
        """
        bye = self.get_parsed_content().get(StreamMessageConstant.BYE, False)
        return bye


class StreamAndPartition(EqualFunc):
    """
    StreamAndPartition class
    """

    def __init__(self, stream_id, stream_partition):
        self.stream_id = stream_id
        self.stream_partition = stream_partition

    def to_object(self, version=28):
        """
        convert StreamAndPartition to dict
        :param version: 28 or 29
        :return: dict
        """
        return {StreamAndPartitionConstant.STREAM_ID: self.stream_id,
                StreamAndPartitionConstant.STREAM_PARTITION: self.stream_partition}

    def serialize(self):
        """
        serialize StreamAndPartition
        :return: str
        """
        return json.dumps(self.to_object())

    @classmethod
    def deserialize(cls, msg):
        """
        convert a str or dict to a StreamAndPartition object
        :param msg: dict
        :return: StreamAndPartition object
        """
        msg = jparser(msg)
        stream_id = msg.get(StreamAndPartitionConstant.STREAM_ID, None)
        stream_partition = msg.get(StreamAndPartitionConstant.STREAM_PARTITION, None)
        return cls(stream_id, stream_partition)


class ResendResponsePayload(StreamAndPartition):
    """
    ResendResponsePayload class
    """

    def __init__(self, stream_id, stream_partition, sub_id):
        super().__init__(stream_id, stream_partition)
        self.sub_id = sub_id

    def to_object(self, version=28):
        """
        convert ResendResponsePayload to dict
        :param version: 28 or 29
        :return: dict
        """
        return {**super().to_object(),
                **{ResendResponsePayloadConstant.SUB_ID: self.sub_id}}

    @classmethod
    def deserialize(cls, msg):
        """
        convert a str or dict to a ResendResponsePayload object
        :param msg: dict
        :return: ResendResponsePayload object
        """
        msg = jparser(msg)
        stream_id = msg.get(ResendResponsePayloadConstant.STREAM_ID, None)
        stream_partition = msg.get(ResendResponsePayloadConstant.STREAM_PARTITION, None)
        sub_id = msg.get(ResendResponsePayloadConstant.SUB_ID, None)
        return cls(stream_id, stream_partition, sub_id)


class ErrorPayload(EqualFunc):
    """
    ErrorPayload class
    """

    def __init__(self, error_string):
        self.error = error_string

    def to_object(self, _=28):
        """
        convert ErrorPayload to dict
        :return: dict
        """
        return {ErrorPayloadConstant.ERROR: self.error}

    @classmethod
    def deserialize(cls, msg):
        """
        convert a str or dict to a ErrorPayload object
        :param msg: dict
        :return: ErrorPayload object
        """
        msg = jparser(msg)
        if not msg.get(ErrorPayloadConstant.ERROR, False):
            raise ValueError('Invalid error payload. received : %s' %
                             (json.dumps(msg)))
        else:
            return ErrorPayload(msg[ErrorPayloadConstant.ERROR])
