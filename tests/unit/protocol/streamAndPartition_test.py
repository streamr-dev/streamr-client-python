"""
test streamAndPartition class
"""


from streamr.protocol.payload import StreamAndPartition
import json


def test_stream_and_partition():
    msg = {
        'stream': 'sub_id',
        'partition': 0,
    }

    sp = StreamAndPartition.deserialize(json.dumps(msg))

    assert isinstance(sp, StreamAndPartition)
    assert sp.stream_id == msg.get('stream', None)
    assert sp.stream_partition == msg.get('partition', None)

    msg = {
        'stream': 'sub_id',
        'partition': 0,
    }

    res = StreamAndPartition('sub_id', 0).serialize()
    assert isinstance(res, str)

    assert msg == json.loads(res)
