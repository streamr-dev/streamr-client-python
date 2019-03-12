"""
test subscribeRequest class
"""


from streamr.protocol.request import SubscribeRequest
import json


msg = {
    'type': 'subscribe',
    'stream': 'stream_id',
    'partition': 0,
    'authKey': 'authKey',
    'sessionToken': 'session_token',
}

result = SubscribeRequest.deserialize(json.dumps(msg))

assert isinstance(result, SubscribeRequest)
assert result.stream_id == 'stream_id'
assert result.stream_partition == 0
assert result.api_key == 'authKey'
assert result.session_token == 'session_token'


serialized = SubscribeRequest(
    'stream_id', 0, 'authKey', 'session_token').serialize()
assert type(serialized) == str
dic = json.loads(serialized)

assert dic == msg

print('test subscribe request passed')