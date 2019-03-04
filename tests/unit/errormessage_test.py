from streamr.protocol.payloads import ErrorPayload
import json

if __name__=='__main__':

    msg = {'error':'foo'}

    result = ErrorPayload.deserialize(json.dumps(msg))

    assert isinstance(result,ErrorPayload)
    assert result.error == msg['error']

    obj = ErrorPayload(msg['error']).toObject()

    assert obj == msg
    