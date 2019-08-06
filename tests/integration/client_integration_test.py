"""
test client
"""


import time
import random

from streamr import Client, Option


def test_client():
    api_key_test = '27ogvnHOQhGFQGETwjf1dAWFd2wXHbTlKCj_uEUTESXw'
    
    option = Option.get_default_option()
    option.auto_connect = False
    option.auto_disconnect = False
    option.api_key = api_key_test

    cli = Client(option)

    while cli.session_token is None:
        pass

    session_token = cli.session_token

    assert session_token is not None

    stream_name = ''.join(random.sample('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890', 20))

    stream = cli.get_or_create_stream(stream_name)
    assert(isinstance(stream, list))
    for s in stream:
        assert s['name'] == stream_name

    stream_by_name = cli.get_stream_by_name(stream_name)
    assert(isinstance(stream_by_name, list))
    for s in stream:
        assert s['name'] == stream_name

    stream_id = stream[0]['id']
    stream_by_id = cli.get_stream_by_id(stream_id)
    assert isinstance(stream_by_id, dict)
    assert stream_by_id['id'] == stream_id

    if not cli.is_connected():
        cli.connect()

    while not cli.is_connected():
        pass
    
    def get_random_name():
        """
        generate random variable
        :return: string
        """
        return ''.join(random.sample('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', 5))
    
    msg = [{"name": get_random_name(), "age": random.randrange(0, 100)},
           {"name": get_random_name(), "age": random.randrange(0, 100)},
           {"name": get_random_name(), "age": random.randrange(0, 100)},
           {"name": get_random_name(), "age": random.randrange(0, 100)}]

    def callback(parsed_msg, msg_object):
        """
        callback function which will be called when subscription received a message
        :param parsed_msg: deserialized message
        :param msg_object: message dict object
        :return:
        """
        assert msg_object.stream_id == stream_id
        assert parsed_msg in msg

    subscription = cli.subscribe(stream_id, callback)

    for m in msg:
        cli.publish(subscription, m)

    time.sleep(30)

    cli.disconnect()
