from streamr.protocol.request import SubscribeRequest
import json


msg = {
    'type': 'subscribe',
    'stream': 'streamId',
    'partition': 0,
    'authKey': 'authKey',
    'sessionToken': 'sessionToken',
}

result = SubscribeRequest.deserialize(json.dumps(msg))

assert isinstance(result, SubscribeRequest)
assert result.streamId == 'streamId'
assert result.streamPartition == 0
assert result.apiKey == 'authKey'
assert result.sessionToken == 'sessionToken'


serialized = SubscribeRequest(
    'streamId', 0, 'authKey', 'sessionToken').serialize()
assert type(serialized) == str
dic = json.loads(serialized)

assert dic == msg
