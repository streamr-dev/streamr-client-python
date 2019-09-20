"""
test resendResponse class
"""


from streamr.protocol.response import Response, ResendResponseNoResend, \
    ResendResponseResending, ResendResponseResent, \
    BroadcastMessage, UnicastMessage, \
    SubscribeResponse, UnsubscribeResponse, \
    ErrorResponse
from streamr.protocol.payload import StreamMessage, StreamAndPartition, ResendResponsePayload, ErrorPayload
import json


def test_response():
    examples_by_type = {
        '0': [0, 0, None, [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                           941516902, 941499898,
                           StreamMessage.ContentType.JSON,
                           '{"valid": "json"}']],
        '1': [0, 1, 'sub_id', [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                               941516902, 941499898,
                               StreamMessage.ContentType.JSON,
                               '{"valid": "json"}']],
        '2': [0, 2, None, {
            'stream': 'stream_id',
            'partition': 0,
        }],
        '3': [0, 3, None, {
            'stream': 'stream_id',
            'partition': 0,
        }],
        '4': [0, 4, None, {
            'stream': 'stream_id',
            'partition': 0,
            'sub': 'sub_id',
        }],
        '5': [0, 5, None, {
            'stream': 'stream_id',
            'partition': 0,
            'sub': 'sub_id',
        }],
        '6': [0, 6, None, {
            'stream': 'stream_id',
            'partition': 0,
            'sub': 'sub_id',
        }],
        '7': [0, 7, None, {
            'error': 'foo',
        }],
    }

    result = Response.deserialize(json.dumps(examples_by_type['0']))
    assert isinstance(result, BroadcastMessage)
    assert isinstance(result.payload, StreamMessage)

    result = Response.deserialize(json.dumps(examples_by_type['1']))
    assert isinstance(result, UnicastMessage)
    assert isinstance(result.payload, StreamMessage)
    assert result.sub_id == 'sub_id'

    result = Response.deserialize(json.dumps(examples_by_type['2']))
    assert isinstance(result, SubscribeResponse)
    assert isinstance(result.payload, StreamAndPartition)

    result = Response.deserialize(json.dumps(examples_by_type['3']))
    assert isinstance(result, UnsubscribeResponse)
    assert isinstance(result.payload, StreamAndPartition)

    result = Response.deserialize(json.dumps(examples_by_type['4']))
    assert isinstance(result, ResendResponseResending)
    assert isinstance(result.payload, ResendResponsePayload)
    assert result.payload.sub_id == 'sub_id'

    result = Response.deserialize(json.dumps(examples_by_type['5']))
    assert isinstance(result, ResendResponseResent)
    assert isinstance(result.payload, ResendResponsePayload)
    assert result.payload.sub_id == 'sub_id'

    result = Response.deserialize(json.dumps(examples_by_type['6']))
    assert isinstance(result, ResendResponseNoResend)
    assert isinstance(result.payload, ResendResponsePayload)
    assert result.payload.sub_id == 'sub_id'

    result = Response.deserialize(json.dumps(examples_by_type['7']))
    assert isinstance(result, ErrorResponse)
    assert isinstance(result.payload, ErrorPayload)
    assert result.payload.error == 'foo'

    serialized = Response.deserialize(examples_by_type['0']).serialize()
    assert isinstance(serialized, str)
    assert examples_by_type['0'] == json.loads(serialized)

    serialized = Response.deserialize(examples_by_type['1']).serialize()
    assert isinstance(serialized, str)
    assert examples_by_type['1'] == json.loads(serialized)

    serialized = Response.deserialize(examples_by_type['2']).serialize()
    assert isinstance(serialized, str)
    assert examples_by_type['2'] == json.loads(serialized)

    serialized = Response.deserialize(examples_by_type['3']).serialize()
    assert isinstance(serialized, str)
    assert examples_by_type['3'] == json.loads(serialized)

    serialized = Response.deserialize(examples_by_type['4']).serialize()
    assert isinstance(serialized, str)
    assert examples_by_type['4'] == json.loads(serialized)

    serialized = Response.deserialize(examples_by_type['5']).serialize()
    assert isinstance(serialized, str)
    assert examples_by_type['5'] == json.loads(serialized)

    serialized = Response.deserialize(examples_by_type['6']).serialize()
    assert isinstance(serialized, str)
    assert examples_by_type['6'] == json.loads(serialized)

    serialized = Response.deserialize(examples_by_type['7']).serialize()
    assert isinstance(serialized, str)
    assert examples_by_type['7'] == json.loads(serialized)
