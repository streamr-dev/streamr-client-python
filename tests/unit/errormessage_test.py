"""
test ErrorMessage object
"""


from streamr.protocol.payload import ErrorPayload
import json

if __name__ == '__main__':

    msg = {'error': 'foo'}

    result = ErrorPayload.deserialize(json.dumps(msg))

    assert isinstance(result, ErrorPayload)
    assert result.error == msg['error']

    obj = ErrorPayload(msg['error']).to_object()

    assert obj == msg

    print('ErrorMessage test passed')
