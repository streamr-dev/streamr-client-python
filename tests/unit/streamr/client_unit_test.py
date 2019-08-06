"""
this module provide integration test for client
"""
from collections import deque
import json


from streamr.client.event import Event
from streamr.client.client import Client
from streamr.util.option import Option
from streamr.util.constant import EventConstant
from streamr.protocol.response import Response
from streamr.protocol.request import SubscribeRequest, UnsubscribeRequest


api_key_test = '27ogvnHOQhGFQGETwjf1dAWFd2wXHbTlKCj_uEUTESXw'


class EventMock(Event):
    """
    create a mock of connection object
    """

    def __init__(self):
        super().__init__()
        self.expectedMessagesToSend = deque([])
        self.state = EventConstant.DISCONNECTED

        def connection_func():
            """
            connect function
            :return:
            """
            self.state = EventConstant.CONNECTING
            self.state = EventConstant.CONNECTED
            self.emit(EventConstant.CONNECTED)

        self.connect = connection_func

        def disconnect_func():
            """
            disconnect func
            :return:
            """
            self.state = EventConstant.DISCONNECTING
            self.state = EventConstant.DISCONNECTED
            self.emit(EventConstant.DISCONNECTED)

        self.disconnect = disconnect_func

        def send_func(msg):
            """
            send function
            :param msg:
            :return:
            """
            next_ = self.expectedMessagesToSend.popleft()
            assert msg == next_

        self.send = send_func

        def expect_func(msg):
            """
            expect the request which will be sent to server
            :param msg:
            :return:
            """
            self.expectedMessagesToSend.append(msg)

        self.expect = expect_func

        def check_func():
            """
            check whether all the request has been sent
            :return:
            """
            assert len(self.expectedMessagesToSend) == 0

        self.check = check_func


def init():
    """
    create client with a mock connection
    :return:
    """

    conn = EventMock()
    option = Option.get_default_option()
    option.auto_connect = False
    option.auto_disconnect = False
    option.api_key = api_key_test
    cli = Client(option, conn)
    return cli, conn


def test_connect():

    cli, conn = init()

    def connected_callback():
        """
        callback function of connected event
        :return:
        """
        print('emit connected event successfully')
    cli.on('connected', connected_callback)

    cli.connect()
    conn.check()
    cli.disconnect()


def test_connect_after_subscribe():
    cli, conn = init()

    def connected_callback():
        """
        callback function of connected event
        :return:
        """
        print('emit connected event successfully')
    cli.on('connected', connected_callback)

    cli.subscribe('stream1', lambda: None)
    conn.expect(SubscribeRequest(
        'stream1', api_key=cli.option.api_key, session_token=cli.session_token))
    cli.connect()
    conn.check()
    cli.disconnect()


def test_subscribe_and_unsubscribe():
    cli, conn = init()

    def connected_callback():
        """
        callback function of connected event
        :return:
        """
        print('emit connected event successfully')
    cli.on('connected', connected_callback)

    cli.connect()
    assert len(cli.sub_by_sub_id) == 0

    conn.expect(SubscribeRequest('stream1', api_key=cli.option.api_key, session_token=cli.session_token))

    subscrip = cli.subscribe('stream1', lambda: None)

    conn.check()
    assert len(cli.sub_by_sub_id) == 1

    assert subscrip.get_state() == EventConstant.SUBSCRIBING

    sub_response = Response.deserialize(json.dumps([0, 2, None, {
        'stream': 'stream1',
        'partition': 0,
    }]))

    conn.emit('SubscribeResponse', sub_response)
    assert subscrip.get_state() == EventConstant.SUBSCRIBED

    conn.expect(UnsubscribeRequest('stream1', api_key=cli.option.api_key, session_token=cli.session_token))
    
    cli.unsubscribe(subscrip)

    conn.check()
    assert len(cli.sub_by_sub_id) == 1

    assert subscrip.get_state() == EventConstant.UNSUBSCRIBING

    unsub_response = Response.deserialize(json.dumps([0, 3, None, {
        'stream': 'stream1',
        'partition': 0,
    }]))
    conn.emit('UnsubscribeResponse', unsub_response)

    assert subscrip.get_state() == EventConstant.UNSUBSCRIBED
    assert len(cli.sub_by_sub_id) == 0

    cli.disconnect()
