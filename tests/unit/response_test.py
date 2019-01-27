from streamr.protocol.response import *
from streamr.protocol.payloads import StreamMessage, StreamAndPartition, ResendResponsePayload, ErrorPayload
import json




examplesByType = {
    '0': [0, 0, None, [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                       941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}']],
    '1': [0, 1, 'subId', [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                          941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}']],
    '2': [0, 2, None, {
        'stream': 'id',
        'partition': 0,
    }],
    '3': [0, 3, None, {
        'stream': 'id',
        'partition': 0,
    }],
    '4': [0, 4, None, {
        'stream': 'id',
        'partition': 0,
        'sub': 'subId',
    }],
    '5': [0, 5, None, {
        'stream': 'id',
        'partition': 0,
        'sub': 'subId',
    }],
    '6': [0, 6, None, {
        'stream': 'id',
        'partition': 0,
        'sub': 'subId',
    }],
    '7': [0, 7, None, {
        'error': 'foo',
    }],
}


result = Response.deserialize(json.dumps(examplesByType['0']))
assert isinstance(result,BroadcastMessage)
assert isinstance(result.payload, StreamMessage)


result = Response.deserialize(json.dumps(examplesByType['1']))
assert isinstance(result,UnicastMessage)
assert isinstance(result.payload, StreamMessage)
assert result.subId == 'subId'


result = Response.deserialize(json.dumps(examplesByType['2']))
assert isinstance(result,SubscribeResponse)
assert isinstance(result.payload,StreamAndPartition)

result = Response.deserialize(json.dumps(examplesByType['3']))
assert isinstance(result, UnsubscribeResponse)
assert isinstance(result.payload,StreamAndPartition)

result = Response.deserialize(json.dumps(examplesByType['4']))
assert isinstance(result,ResendResponseResending)
assert isinstance(result.payload,ResendResponsePayload)
assert result.payload.subId == 'subId'


result=Response.deserialize(json.dumps(examplesByType['5']))
assert isinstance(result,ResendResponseResent)
assert isinstance(result.payload,ResendResponsePayload)
assert result.payload.subId == 'subId'

result=Response.deserialize(json.dumps(examplesByType['6']))
assert isinstance(result,ResendResponseNoResend)
assert isinstance(result.payload,ResendResponsePayload)
assert result.payload.subId == 'subId'



result=Response.deserialize(json.dumps(examplesByType['7']))
assert isinstance(result,ErrorResponse)
assert isinstance(result.payload,ErrorPayload)
assert result.payload.error == 'foo'



serialized = Response.deserialize(examplesByType['0']).serialize()
assert type(serialized) == str
assert examplesByType['0'] == json.loads(serialized)

serialized = Response.deserialize(examplesByType['1']).serialize()
assert type(serialized) == str
assert examplesByType['1'] ==  json.loads(serialized)



serialized = Response.deserialize(examplesByType['2']).serialize()
assert type(serialized) == str
assert examplesByType['2'] ==  json.loads(serialized)

serialized = Response.deserialize(examplesByType['3']).serialize()
assert type(serialized) == str
assert examplesByType['3'] ==  json.loads(serialized)



serialized = Response.deserialize(examplesByType['4']).serialize()
assert type(serialized) == str
assert examplesByType['4'] == json.loads(serialized)

serialized = Response.deserialize(examplesByType['5']).serialize()
assert type(serialized) == str
assert examplesByType['5'] == json.loads(serialized)

serialized = Response.deserialize(examplesByType['6']).serialize()
assert type(serialized) == str
assert examplesByType['6'] == json.loads(serialized)



serialized = Response.deserialize(examplesByType['7']).serialize()
assert type(serialized) == str
assert examplesByType['7'] == json.loads(serialized)

