"""
test PublishRequest
"""


from streamr.protocol.request import PublishRequest
import json


def test_publish_request():
    
    msg = {
        "type": 'publish',
        "stream": 'streamId',
        "authKey": 'authKey',
        "sessionToken": 'sessionToken',
        "msg": json.dumps({"foo": 'bar'}),
        "ts": 1533924184016,
        "pkey": 'deviceId',
        "addr": 'publisherAddress',
        "sigtype": 1,
        "sig": 'signature',
    }

    result = PublishRequest.deserialize(json.dumps(msg))

    assert(isinstance(result, PublishRequest))
    assert(result.stream_id == msg['stream'])
    assert(result.api_key == msg['authKey'])
    assert(result.session_token == msg['sessionToken'])
    assert(result.content == msg['msg'])
    assert(result.timestamp == msg['ts'])
    assert(result.partition_key == msg['pkey'])
    assert(result.publisher_address == msg['addr'])
    assert(result.signature_type == msg['sigtype'])
    assert(result.signature == msg['sig'])

    msg = {
        "type": 'publish',
        "stream": 'stream_id123',
        "authKey": 'authKey122',
        "sessionToken": 'sessionToken11',
        "msg": '{}',
        "ts": 1533924184016,
        "pkey": 'deviceId',
        "addr": 'publisherAddress',
        "sigtype": 1,
        "sig": 'signature',
    }

    serial = PublishRequest('stream_id123', 'authKey122', 'sessionToken11', {
    }, 1533924184016, 'deviceId', 'publisherAddress', 1, 'signature').serialize()

    assert isinstance(serial, str)

    dic = json.loads(serial)

    assert msg == dic
