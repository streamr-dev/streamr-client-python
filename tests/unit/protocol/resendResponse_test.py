"""
test the resendResponse class
"""


from streamr.protocol.payload import ResendResponsePayload
import json


def test_resend_response_payload():
    msg = {
        'stream': 'sub_id',
        'partition': 0,
        'sub': 0}

    payload = ResendResponsePayload.deserialize(json.dumps(msg))
    assert isinstance(payload, ResendResponsePayload)
    assert payload.stream_id == msg.get('stream', None)
    assert payload.stream_partition == msg.get('partition', None)
    assert payload.sub_id == msg.get('sub', None)

    msg = {
        'stream': 'sub_id',
        'partition': 0,
        'sub': 0,
    }

    serialized = ResendResponsePayload('sub_id', 0, 0).serialize()

    assert isinstance(serialized, str)

    assert msg == json.loads(serialized)
