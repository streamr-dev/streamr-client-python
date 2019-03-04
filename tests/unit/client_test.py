import logging
from collections import deque
import getopt
import sys
from pathlib import Path
import json

from streamr.client.connection import Connection
from streamr.client.subscription import Subscription
from streamr.client.event import Event
from streamr.client.client import Client
from streamr.protocol.response import SubscribeResponse,UnsubscribeResponse,Response,BroadcastMessage,UnicastMessage
from streamr.protocol.request import SubscribeRequest,UnsubscribeRequest,Request

from tests.config import getAPIKey

logging.basicConfig(level=logging.INFO, filename='mylog.log',
                    format='%(relativeCreated)6d %(threadName)s %(levelname)s :%(message)s')

logger = logging.getLogger(__name__)



def createConnectionMock():

    c = Event()

    c.expectedMessagesToSend = deque([])

    c.state = Connection.State.DISCONNECTED

    def connection():
        logger.warning('Connection mock : connecting')
        c.state = Connection.State.CONNECTING
        logger.warning('Connection mock: connected')
        c.state = Connection.State.CONNECTED
        c.emit('connected')
    c.connect = connection


    def disconnect():
        logger.warning('Connection mock : disconnecting')
        c.state = Connection.State.DISCONNECTING
        logger.warning('Connection mock : disconnected')
        c.state = Connection.State.DISCONNECTED
        c.emit('disconnected')
    c.disconnect = disconnect

    def send(msg):
        next_ = c.expectedMessagesToSend.popleft()
        assert msg == next_
    c.send = send

    def expect(msg):
        c.expectedMessagesToSend.append(msg)
    c.expect = expect

    def check():
        assert len(c.expectedMessagesToSend) == 0
    c.check = check

    return c


def init():

    conn = createConnectionMock()
    cli = Client({'autoConnect': False, 'autoDisconnect': False,
                  'apiKey': getAPIKey()}, conn)
    return cli, conn


def testConnect():

    cli, conn = init()

    def connectedCallback():
        print('emit connected event successfully')
    cli.on('connected', connectedCallback)

    cli.connect()
    conn.check()
    cli.disconnect()


def testConenctAfterSubscribe():
    cli, conn = init()

    def connectedCallback():
        print('emit connected event successfully')
    cli.on('connected', connectedCallback)

    cli.subscribe('stream1', lambda: None)
    conn.expect(SubscribeRequest(
        'stream1', apiKey=cli.options['apiKey'], sessionToken=cli.sessionToken))
    cli.connect()
    conn.check()
    cli.disconnect()


def testSubscribeAndUnsubscribe():
    cli, conn = init()

    def connectedCallback():
        print('emit connected event successfully')
    cli.on('connected', connectedCallback)

    cli.connect()
    assert len(cli.subById) == 0

    conn.expect(SubscribeRequest('stream1', apiKey=cli.options['apiKey'], sessionToken=cli.sessionToken))

    subscrip = cli.subscribe('stream1', lambda: None)

    conn.check()
    assert len(cli.subById) == 1

    assert subscrip.getState() == Subscription.State.SUBSCRIBING

    SubResponse = Response.deserialize(json.dumps([0, 2, None, {
        'stream': 'stream1',
        'partition': 0,
    }]))

    conn.emit('SubscribeResponse', SubResponse)
    assert subscrip.getState() == Subscription.State.SUBSCRIBED

    conn.expect(UnsubscribeRequest('stream1',apiKey=cli.options['apiKey'],sessionToken=cli.sessionToken))
    
    cli.unsubscribe(subscrip)

    conn.check()
    assert len(cli.subById) == 1

    assert subscrip.getState() == Subscription.State.UNSUBSCRIBING

    UnsubResponse = Response.deserialize(json.dumps([0, 3, None, {
        'stream': 'stream1',
        'partition': 0,
    }]))
    conn.emit('UnsubscribeResponse', UnsubResponse)

    assert subscrip.getState() == Subscription.State.UNSUBSCRIBED
    assert len(cli.subById) == 0

    cli.disconnect()


if __name__=='__main__':
    testConnect()
    testConenctAfterSubscribe()
    testSubscribeAndUnsubscribe()
