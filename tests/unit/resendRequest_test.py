from streamr.protocol.request import ResendRequest
import json

msg = {
    'type': 'resend',
    'stream': 'id',
    'partition': 0,
    'sub': 'subId',
    'resend_all': True,
}

rest = ResendRequest.deserialize(json.dumps(msg))

assert(isinstance(rest,ResendRequest))
assert(rest.streamId == msg['stream'])
assert(rest.streamPartition == msg['partition'])
assert(rest.subId == msg['sub'])

assert rest.resendOptions == {'resend_all':True}

msg = {
    'type': 'resend',
    'stream': 'id',
    'partition': 0,
    'sub': 'subId',
    'resend_all': True,
    'sessionToken':None,
    'authKey':None
}

serialized = ResendRequest('id', 0, 'subId', {'resend_all': True}).serialize()

assert(isinstance(serialized, str))

dic = json.loads(serialized)
assert dic == msg

