"""
test unsubscribeRequest
"""


from streamr.protocol.request import UnsubscribeRequest
import json

msg = {
    'type': 'unsubscribe',
    'stream': 'sub_id',
    'partition': 0,
    'authKey': 'authKey',
    'session_token': 'session_token',
}
result = UnsubscribeRequest.deserialize(json.dumps(msg))

assert isinstance(result, UnsubscribeRequest)
assert result.stream_id == msg['stream']
assert result.stream_partition == msg['partition']

msg = {
    'type': 'unsubscribe',
    'stream': 'sub_id',
    'partition': 0,
    'authKey': 'authKey',
    'sessionToken': 'session_token',
}

serialized = UnsubscribeRequest('sub_id', 0, 'authKey', 'session_token').serialize()
dic = json.loads(serialized)


assert type(serialized) == str

assert dic == msg

print('test unsubscribeRequest passed')
