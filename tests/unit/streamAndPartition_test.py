from streamr.protocol.payloads import StreamAndPartition
import json

msg = {
    'stream': 'id',
    'partition': 0,
}

sp = StreamAndPartition.deserialize(json.dumps(msg))

assert isinstance(sp, StreamAndPartition)
assert sp.streamId == msg.get('stream', None)
assert sp.streamPartition == msg.get('partition', None)


msg = {
    'stream': 'id',
    'partition': 0,
}

res = StreamAndPartition('id', 0).serialize()
assert type(res) == str

assert msg == json.loads(res)
