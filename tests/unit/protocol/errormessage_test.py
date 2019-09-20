"""
test ErrorMessage class
"""


from streamr.protocol.payload import ErrorPayload
import json


def test_error_payload():

    msg = {'error': 'foo'}

    result = ErrorPayload.deserialize(json.dumps(msg))

    assert isinstance(result, ErrorPayload)
    assert result.error == msg['error']

    obj = ErrorPayload(msg['error']).to_object()

    assert obj == msg

