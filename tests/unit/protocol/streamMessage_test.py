"""
test streamMessage class
"""


from streamr.protocol.payload import StreamMessage
from streamr.protocol.errors.error import InvalidJsonError, UnsupportedVersionError
import json
import time


def test_stream_message_v28():

    # streammessage version 28 deserialize
    arr = [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
           941516902, 941499898, StreamMessage.ContentType.JSON, '{"valid": "json"}']

    msg = StreamMessage.deserialize(arr)

    assert isinstance(msg, StreamMessage)
    assert msg.stream_id == 'TsvTbqshTsuLg_HyUjxigA'
    assert msg.stream_partition == 0
    assert msg.timestamp == 1529549961116
    assert msg.ttl == 0
    assert msg.offset == 941516902
    assert msg.previous_offset == 941499898
    assert msg.content_type == StreamMessage.ContentType.JSON
    assert msg.content == '{"valid": "json"}'

    # streammessage version 28  content invalid
    arr = [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
           941516902, 941499898, StreamMessage.ContentType.JSON, '{"invalid\njson"}']

    try:
        StreamMessage.deserialize(arr)
    except InvalidJsonError as e:
        assert isinstance(e, InvalidJsonError)
        assert e.stream_id == 'TsvTbqshTsuLg_HyUjxigA'
        assert e.json_string == '{"invalid\njson"}'
        assert isinstance(e.stream_message, StreamMessage)

    # streammessage version 28  serialize
    arr = [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
           941516902, 941499898, StreamMessage.ContentType.JSON, '{"valid": "json"}']
    serialized = StreamMessage(
        'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
        941516902, 941499898, StreamMessage.ContentType.JSON,
        '{"valid": "json"}').serialize(28)
    assert serialized == json.dumps(arr)


def test_stream_message_v29():
    # streammessage version 29  deserialize
    arr = [29, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
           941516902, 941499898, StreamMessage.ContentType.JSON,
           '{"valid": "json"}', 1, 'address', 'signature']

    result = StreamMessage.deserialize(arr)

    assert isinstance(result, StreamMessage)

    assert result.stream_id == 'TsvTbqshTsuLg_HyUjxigA'
    assert result.stream_partition == 0
    assert result.timestamp == 1529549961116
    assert result.ttl == 0
    assert result.offset == 941516902
    assert result.previous_offset == 941499898
    assert result.content_type == StreamMessage.ContentType.JSON
    assert result.content == '{"valid": "json"}'
    assert result.signature_type == 1
    assert result.publisher_address == 'address'
    assert result.signature == 'signature'

    # streammessage version 29  serialize
    arr = [29, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
           941516902, 941499898, StreamMessage.ContentType.JSON, '{"valid": "json"}', 1, 'address', 'signature']
    serialized = StreamMessage('TsvTbqshTsuLg_HyUjxigA', 0,
                               1529549961116, 0, 941516902, 941499898,
                               StreamMessage.ContentType.JSON, '{"valid": "json"}',
                               1, 'address', 'signature').serialize(29)
    assert serialized == json.dumps(arr)


def test_unsupported_version():
    # unsupported version deserialize
    arr = [123]

    try:
        StreamMessage.deserialize(arr)
    except UnsupportedVersionError as e:
        assert isinstance(e, UnsupportedVersionError)
        assert e.version == 123

    # unsupported version serialize
    try:
        StreamMessage('TsvTbqshTsuLg_HyUjxigA', 0,
                      1529549961116, 0, 941516902, 941499898,
                      StreamMessage.ContentType.JSON, '{"valid":"json"}',
                      1, 'address', 'signature').serialize(123)
    except UnsupportedVersionError as e:
        assert isinstance(e, UnsupportedVersionError)
        assert e.version == 123


def test_get_parsed_content():
    # get_parsed_content
    content = {'foo': 'bar'}
    msg = StreamMessage('streamId', 0, time.time(), 0, 1, None,
                        StreamMessage.ContentType.JSON, json.dumps(content))

    dic = msg.get_parsed_content()
    assert dic == content

    content = {'foo': 'bar'}
    msg = StreamMessage('streamId', 0, time.time(), 0, 1,
                        None, StreamMessage.ContentType.JSON, content)
    dic = msg.get_parsed_content()
    assert dic == content


def test_to_object():
    # to object parse is true
    obj = [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
           941516902, 941499898, StreamMessage.ContentType.JSON, {
               'valid': 'json',
           }]

    msg = StreamMessage(
        'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
        941516902, 941499898, StreamMessage.ContentType.JSON, '{"valid": "json"}',
    )
    lizt = msg.to_object(28, True)

    assert obj == lizt


def test_to_ojbect_compact_is_false():

    # to object  compact is false
    obj = {
        'streamId': 'TsvTbqshTsuLg_HyUjxigA',
        'streamPartition': 0,
        'timestamp': 1529549961116,
        'ttl': 0,
        'offset': 941516902,
        'previousOffset': 941499898,
        'contentType': StreamMessage.ContentType.JSON,
        'content': '{"valid": "json"}'
    }

    msg = StreamMessage(
        'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
        941516902, 941499898, StreamMessage.ContentType.JSON, '{"valid": "json"}'
    )

    dic = msg.to_object(28, False, False)

    assert dic == obj


def test_to_ojbect_compact_is_true():
    # to object  compact is false parse is True
    obj = {
        'streamId': 'TsvTbqshTsuLg_HyUjxigA',
        'streamPartition': 0,
        'timestamp': 1529549961116,
        'ttl': 0,
        'offset': 941516902,
        'previousOffset': 941499898,
        'contentType': StreamMessage.ContentType.JSON,
        'content': {
            'valid': 'json'
        }}

    msg = StreamMessage(
        'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
        941516902, 941499898, StreamMessage.ContentType.JSON, '{"valid": "json"}'
    )

    dic = msg.to_object(28, True, False)

    assert dic == obj
