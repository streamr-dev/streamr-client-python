"""
test ResendRequest
"""


from streamr.protocol.request import ResendRequest
from streamr.util.option import Option
import json


def test_resend_request():
    msg = {
        'sessionToken': 'my_sessionToken',
        'authKey': 'authKey',
        'type': 'resend',
        'stream': 'id',
        'partition': 0,
        'sub': 'subId',
        'resend_all': True,
    }

    rest = ResendRequest.deserialize(json.dumps(msg))

    assert isinstance(rest, ResendRequest)
    assert rest.stream_id == msg['stream']
    assert rest.stream_partition == msg['partition']
    assert rest.sub_id == msg['sub']
    assert rest.resend_option == Option(resend_all=True)
    assert rest.api_key == msg['authKey']
    assert rest.session_token == msg['sessionToken']

    msg = {
        'type': 'resend',
        'stream': 'id',
        'partition': 0,
        'sub': 'subId',
        'resend_all': True,
        'sessionToken': 'my_sessionToken',
        'authKey': 'authKey'
    }

    serialized = ResendRequest('id', 0, 'subId', Option(resend_all=True), 'authKey', 'my_sessionToken').serialize()
    assert(isinstance(serialized, str))
    
    dic = json.loads(serialized)
    assert dic == msg
