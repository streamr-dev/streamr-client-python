import logging
from collections import deque
import getopt
import sys

from streamr.client.connection import Connection
from streamr.protocol.payloads import *
from streamr.protocol.request import *
from streamr.protocol.response import *
from streamr.client.event import Event
from streamr.client.client import Client



def msg(streamId='stream1', offset=0, content={}, subId=None):
    import time
    if subId == None:
        return UnicastMessage(StreamMessage(streamId, 0, time.time(), 0, offset, None, StreamMessage.CONTENT_TYPE.JSON, content), subId)
    else:
        return BroadcastMessage(StreamMessage(streamId, 0, time.time(), 0, offset, None, StreamMessage.CONTENT_TYPE.JSON, content))


def createConnectionMock():
    
    c = Event()
    c.expectedMessagesToSend = deque([])

    c.state = Connection.State.DISCONNECTED

    def a1():
        logging.warning('Connection mock : connecting')
        c.state = Connection.State.CONNECTING
        logging.warning('Connection mock: connected')
        c.state = Connection.State.CONNECTED
        c.emit('connected')
    c.connect = a1

    def a2():
        print(len(c.expectedMessagesToSend))
    c.checkSentMessage = a2

    def a3():
        logging.warning('Connection mock : disconnecting')
        c.state = Connection.State.DISCONNECTING
        logging.warning('Connection mock: disconnected')
        c.state = Connection.State.DISCONNECTED
        c.emit('disconnected')
    c.disconnect = a3

    def a4(msg):
        next_ = c.expectedMessagesToSend.popleft()
        assert msg == next_
    c.send = a4

    def a5(payload):
        c.emit(payload.getMessageName(),payload)
    c.emitMessge = a5

    def a6(msg):
        c.expectedMessagesToSend.append(msg)
    c.expect = a6


    return c


if __name__ == '__main__':

    conn = createConnectionMock()

    cli = Client({'autoConnect': False, 'autoDisconnect': False}, conn)

    if len(sys.argv) > 1:
        optlist, args = getopt.getopt(sys.argv[1:], 'i:j:k:')

    for m,t in optlist:
        if m == '-i':
            t1 = t
        elif m =='-j':
            t2 = t
        elif m =='-k':
            t3 = t
    
    if t1 == 'connection_event_handling':
        if t2 == 'connected':
            if t3 == 'emit_event_on_client':
                def emp():
                    print('emp')

                cli.on('connected', emp)
                cli.connect()
            elif t3 == 'event_nothing_if_not_subscribed_to_anything':
                cli.connect()
                conn.on('connected',lambda : None)
            elif t3 == 'send_pending_subscribes':
                cli.subscribe('stream1', lambda: None)
                conn.expect(SubscribeRequest('stream1'))
                cli.connect()
                conn.on('connected', lambda: None)
            elif t3 == 'send_pending_when_reconnected':
                conn.expect(SubscribeRequest('stream1'))
                conn.expect(SubscribeRequest('stream1'))
                ss = cli.subscribe('stream1',lambda : None)
                cli.connect()
                conn.disconnect()
                cli.connect()
            elif t3 == 'not_subscribe_to_unsubsubs_when_recon':
                conn.expect(SubscribeRequest('stream1'))
                conn.expect(UnsubscribeRequest('stream1'))
                sub = cli.subscribe('stream1',lambda : None)
                cli.connect()
                conn.emitMessge(SubscribeResponse(sub.streamId))
                def func():
                    cli.disconnect()
                    cli.connect()
                sub.on('unsubscribed',func)
                cli.unsubscribe(sub)
                cli.connection.emitMessge(UnsubscribeResponse(sub.streamId))

    conn.checkSentMessage()
