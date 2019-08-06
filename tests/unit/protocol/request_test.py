"""
request test
"""


from streamr.protocol.request import Request, SubscribeRequest

import json


def test_request():
    msg = {
        'type': 'unsubscribe',
        'stream': 'stream_id',
        'authKey': 'authKey',
        'sessionToken': 'session_token',
    }

    serialized = Request(msg.get('type', None), msg.get('stream', None),
                         msg.get('authKey', None),
                         msg.get('sessionToken', None)).serialize()
    assert isinstance(serialized, str)
    dic = json.loads(serialized)
    assert dic == msg

    msg = SubscribeRequest('stream_id', 1, 'authKey')
    result = Request.deserialize(msg.serialize())

    assert isinstance(result, SubscribeRequest)
    assert msg.stream_id == result.stream_id
    assert msg.api_key == result.api_key
    assert msg.request_type == result.request_type
    assert msg.stream_partition == result.stream_partition
    assert msg.session_token == result.session_token

