"""
test client
"""


import logging
import time

from streamr.client.client import Client
from streamr.util.option import Option
from tests.config import get_api_key


def test_client():
    logging.basicConfig(level=logging.ERROR, 
                        format='%(relativeCreated)6d %(threadName)s %(levelname)s :%(message)s')

    option = Option.get_default_option()
    option.auto_connect = False
    option.auto_disconnect = False
    option.api_key = get_api_key()

    cli = Client(option)

    while cli.session_token is None:
        pass

    session_token = cli.session_token

    assert session_token is not None

    stream = cli.get_or_create_stream('stream-test')
    assert(isinstance(stream, list))
    for s in stream:
        assert s['name'] == 'stream-test'

    stream_by_name = cli.get_stream_by_name('stream-test')
    assert(isinstance(stream_by_name, list))
    for s in stream:
        assert(s['name'] == 'stream-test')

    stream_id = stream[0]['id']
    stream_by_id = cli.get_stream_by_id(stream_id)
    assert(isinstance(stream_by_id, dict))
    assert(stream_by_id['id'] == stream_id)

    if not cli.is_connected():
        cli.connect()

    while not cli.is_connected():
        pass

    msg = [{"name": 'google', "age": 19}, {"name": "facebook", "age": 11},
           {"name": "yahoo", "age": 13}, {"name": "twitter", "age": 1}]

    counts = 0

    def callback(parsed_msg, msg_object):
        """
        callback function which will be called when subscription received a message
        :param parsed_msg: deserialized messge
        :param msg_object: message dict object
        :return:
        """
        assert msg_object.stream_id == stream_id
        assert parsed_msg in msg
        nonlocal counts
        counts += 1

    subscription = cli.subscribe(stream_id, callback)

    for m in msg:
        cli.publish(subscription, m)

    time.sleep(30)
    assert(counts == 4)
    cli.disconnect()

    print('tests passed')


if __name__ == "__main__":
    test_client()
