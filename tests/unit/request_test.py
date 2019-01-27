from streamr.protocol.request import Request, SubscribeRequest

import json

msg = {
'type': 'unsubscribe',
'stream': 'id',
'authKey': 'authKey',
'sessionToken': 'sessionToken',
}

serialized = Request(msg.get('type',None),msg.get('stream',None),msg.get('authKey',None),msg.get('sessionToken',None)).serialize()
assert type(serialized) == str
dic = json.loads(serialized)

assert dic == msg

msg = SubscribeRequest('streamId',1,'apiKey')
result = Request.deserialize(msg.serialize())

assert isinstance(result,SubscribeRequest)
assert msg.streamId == result.streamId
assert msg.apiKey == result.apiKey
assert msg.requestType == result.requestType
assert msg.streamPartition == result.streamPartition
assert msg.sessionToken == result.sessionToken

