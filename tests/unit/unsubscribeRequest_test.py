from streamr.protocol.request import UnsubscribeRequest
import json

msg = {
    'type': 'unsubscribe',
    'stream': 'id',
    'partition': 0,
    'authKey': 'authKey',
    'sessionToken': 'sessionToken',
}
result = UnsubscribeRequest.deserialize(json.dumps(msg))

assert isinstance(result, UnsubscribeRequest)
assert result.streamId == msg['stream']
assert result.streamPartition == msg['partition']

msg = {
    'type': 'unsubscribe',
    'stream': 'id',
    'partition': 0,
    'authKey': 'authKey',
    'sessionToken': 'sessionToken',
}

serialized = UnsubscribeRequest('id', 0, 'authKey', 'sessionToken').serialize()
dic = json.loads(serialized)


assert type(serialized) == str

assert dic == msg
