from streamr.protocol.payloads import ResendResponsePayload
import json

msg = {
    'stream': 'id',
    'partition': 0,
    'sub':0}

payload = ResendResponsePayload.deserialize(json.dumps(msg))
assert isinstance(payload,ResendResponsePayload)
assert payload.streamId == msg.get('stream',None)
assert payload.streamPartition == msg.get('partition',None)
assert payload.subId == msg.get('sub',None)


msg = {
    'stream': 'id',
    'partition': 0,
    'sub': 0,
}


serialized = ResendResponsePayload('id',0,0).serialize()

assert type(serialized) == str
assert msg == json.loads(serialized)
