"""
this module provide intergration test for client
"""
import logging
from collections import deque
import json

from streamr.client.connection import Connection
from streamr.client.subscription import Subscription
from streamr.client.event import Event
from streamr.client.client import Client
from streamr.util.option import Option
from streamr.protocol.response import Response
from streamr.protocol.request import SubscribeRequest, UnsubscribeRequest

from tests.config import get_api_key

logging.basicConfig(level=logging.INFO, filename='mylog.log',
                    format='%(relativeCreated)6d %(threadName)s %(levelname)s :%(message)s')

logger = logging.getLogger(__name__)


class EventMock(Event):
    """
    create a mock of connection object
    """

    def __init__(self):
        super().__init__()
        self.expectedMessagesToSend = deque([])
        self.state = Connection.State.DISCONNECTED

        def connection_func():
            """
            connect function
            :return:
            """
            logger.warning('Connection mock : connecting')
            self.state = Connection.State.CONNECTING
            logger.warning('Connection mock: connected')
            self.state = Connection.State.CONNECTED
            self.emit('connected')

        self.connect = connection_func

        def disconnect_func():
            """
            disconnect func
            :return:
            """
            logger.warning('Connection mock : disconnecting')
            self.state = Connection.State.DISCONNECTING
            logger.warning('Connection mock : disconnected')
            self.state = Connection.State.DISCONNECTED
            self.emit('disconnected')

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
    option.api_key = get_api_key()
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


def test_conenct_after_subscribe():
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

    assert subscrip.get_state() == Subscription.State.SUBSCRIBING

    sub_response = Response.deserialize(json.dumps([0, 2, None, {
        'stream': 'stream1',
        'partition': 0,
    }]))

    conn.emit('SubscribeResponse', sub_response)
    assert subscrip.get_state() == Subscription.State.SUBSCRIBED

    conn.expect(UnsubscribeRequest('stream1', api_key=cli.option.api_key, session_token=cli.session_token))
    
    cli.unsubscribe(subscrip)

    conn.check()
    assert len(cli.sub_by_sub_id) == 1

    assert subscrip.get_state() == Subscription.State.UNSUBSCRIBING

    unsub_response = Response.deserialize(json.dumps([0, 3, None, {
        'stream': 'stream1',
        'partition': 0,
    }]))
    conn.emit('UnsubscribeResponse', unsub_response)

    assert subscrip.get_state() == Subscription.State.UNSUBSCRIBED
    assert len(cli.sub_by_sub_id) == 0

    cli.disconnect()


if __name__ == '__main__':
    test_connect()
    test_conenct_after_subscribe()
    test_subscribe_and_unsubscribe()
    print('client test passed')
