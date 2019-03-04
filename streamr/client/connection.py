from streamr.client.event import Event
from streamr.protocol.response import Response
from streamr.protocol.request import Request
from streamr.client.errors.error import ConnectionError, ParameterError, ConnectionFailedError, MessageError
from streamr.client.util.websock import MyWebSocket
import websocket
import logging
import websocket
import threading


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
        DISCONNECTED = 'DISCONNECTED'
        CONNECTING = 'CONNECTING'
        CONNECTED = 'CONNECTED'
        DISCONNECTING = 'DISCONNECTING'

    def __init__(self, options, socket=None):
        super().__init__()

        self.retryCounter = 0

        if not isinstance(options, dict):
            raise ParameterError('options should be a dict')

        if options.get('url', None) is None:
            raise ParameterError('URL is not defined')

        self.options = options
        self.state = Connection.State.DISCONNECTED

        self.socket = socket if isinstance(
            socket, MyWebSocket) else MyWebSocket(self.options['url'])

        def socketOpenCallback(ws):
            logger.debug('Connected to %s' % self.options['url'])
            self.updateState(self.State.CONNECTED)
        self.socket.on_open = socketOpenCallback

        def socketCloseCallback(ws):
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
        self.socket.on_close = socketCloseCallback

        def socketMessageCallback(ws, origMsg):
            try:
                msg = Response.deserialize(origMsg)
                print(msg.getMessageName())
                self.emit(msg.getMessageName(), msg)
                logger.info('get %s response' % (msg.getMessageName()))
            except (MessageError,Exception) as e:
                if isinstance(e,MessageError):
                    self.emit('error',MessageError(msg))
                else:
                    self.emit('error',e)
        self.socket.on_message = socketMessageCallback

        def socketErrorCallback(ws, error):
            self.emit('error', error)
        self.socket.on_error = socketErrorCallback

        if options.get('autoConnect', False):
            self.connect()

    def updateState(self, state):
        self.state = state
        logger.debug('Connection state:%s' % (self.state))
        print('Client state : %s' % (self.state))
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
                self.emit('error', e)
        thread = threading.Thread(target=run)
        thread.start()

    def disconnect(self):
        if self.state == self.State.DISCONNECTING:
            raise ConnectionError('Already disconnecting')
        elif self.state == self.State.DISCONNECTED:
            raise ConnectionError('Already disconnected')

        self.updateState(self.State.DISCONNECTING)
        self.socket.close()

    def send(self, request):
        try:
            self.socket.send(request.serialize())
        except Exception as e:
            self.emit('error', e)
