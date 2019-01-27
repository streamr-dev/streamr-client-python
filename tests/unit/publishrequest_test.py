from streamr.protocol.request import PublishRequest
import json

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

assert( isinstance(result,PublishRequest))
assert( result.streamId == msg['stream'])
assert( result.apiKey == msg['authKey'])
assert( result.sessionToken == msg['sessionToken'])
assert( result.content == msg['msg'])
assert( result.timestamp == msg['ts'])
assert( result.partitionKey == msg['pkey'])
assert( result.publisherAddress == msg['addr'])
assert( result.signatureType == msg['sigtype'])
assert( result.signature == msg['sig'])



msg = {
    "type": 'publish',
    "stream": 'streamId123',
    "authKey": 'authKey122',
    "sessionToken": 'sessionToken11',
    "msg": '{}',
    "ts": 1533924184016,
    "pkey": 'deviceId',
    "addr": 'publisherAddress',
    "sigtype": 1,
    "sig": 'signature',
}

serial = PublishRequest('streamId123', 'authKey122', 'sessionToken11', {}, 1533924184016,'deviceId','publisherAddress',1,'signature').serialize()

assert( type(serial) == str)

dic = json.loads(serial)

assert msg == dic