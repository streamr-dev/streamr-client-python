from streamr.client.event import Event
from streamr.protocol.response import Response
from streamr.protocol.request import Request
from streamr.client.errors.error import ConnectionError
from streamr.client.util.websock import MyWebSocket
import websocket
import logging
import websocket
import threading
import time
import os
import json


__all___ = ['Connection']

logger = logging.getLogger(__name__)


class Connection(Event):
    """
    This class constructs a connection object

    paras:
        options: dictionary
        {"url": 'wss://'}

    methods:
        connect()
            connect to websock server

        disconnect()
            disconnect from websock server

        send()
            send request to websock server

    """

    class State:
        DISCONNECTED = 'disconnected'
        CONNECTING = 'connecting'
        CONNECTED = 'connected'
        DISCONNECTING = 'disconnecting'

    def __init__(self,options,socket=None):
        super().__init__()

        self.retryCounter = 0

        if type(options) != dict:
            raise ConnectionError('options should be a dict')

        if options.get('url',None) == None:
            raise ConnectionError('URL is not defined')

        self.options = options
        self.state = Connection.State.DISCONNECTED
        
        self.socket = socket if isinstance(socket, MyWebSocket) else MyWebSocket(self.options['url'])
        
        def socket_open(ws):
            logger.debug('Connected to %s' % self.options['url'])
            self.updateState(self.State.CONNECTED)
        self.socket.on_open = socket_open

        def socket_close(ws):
            if self.state != self.State.DISCONNECTING:
                self.retryCounter += 1
                logger.error('Connection lost. Attempting to reconnect')
                self.updateState(self.State.DISCONNECTED)
                if self.retryCounter <= 10:
                    self.connect()
                else:
                    logger.error(
                        'Reconnect failed for 10 times. Stop reconnecting')
            else:
                self.updateState(self.State.DISCONNECTED)
        self.socket.on_close = socket_close

        def socket_message(ws, msg):
            try:
                msg = Response.deserialize(msg)
                self.emit(msg.getMessageName(), msg)
                logger.debug('get %s response'%(msg.getMessageName))
            except Exception as e:
                self.emit('error', e)
        self.socket.on_message = socket_message

        def socket_error(ws, error):
            self.emit('error', error)
            import traceback
            traceback.print_exc()
        self.socket.on_error = socket_error

        if options.get('autoConnect',False):
            self.connect()

    def updateState(self,state):
        self.state = state
        logger.debug('Connection state:%s'%(self.state))
        print('Client state : %s'%(self.state))
        self.emit(self.state)

    def connect(self):

        if self.retryCounter > 10:
            self.retryCounter = 0
        
        if self.state == self.State.CONNECTING:
            raise ConnectionError('Already connecting')

        elif self.state == self.State.CONNECTED:
            raise ConnectionError('Already connected')

        self.updateState(Connection.State.CONNECTING)
       
        def run():
            try:
                self.socket.run_forever()
            except Exception as e:
                raise Exception('websock error: %s' % e)
        thread = threading.Thread(target=run)
        thread.start()

    def disconnect(self):
        if self.state == self.State.DISCONNECTING:
            raise ConnectionError('Already disconnecting')
        elif self.state == self.State.DISCONNECTED:
            raise ConnectionError('Already disconnected')

        self.updateState(self.State.DISCONNECTING)
        self.socket.close()

    def send(self,request):
        try:
            self.socket.send(request.serialize())
        except Exception as e:
            self.emit('error',e)
