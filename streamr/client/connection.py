"""
this module provide the connection class
"""


from streamr.client.event import Event
from streamr.util.option import Option
from streamr.util.constant import EventConstant
from streamr.protocol.response import Response
from streamr.client.errors.error import ConnectionErr
from streamr.client.util.websock import MyWebSocket

import logging
import threading


__all___ = ['Connection']

logger = logging.getLogger(__name__)


class Connection(Event):
    """
    Connection class
    """

    def __init__(self, option, socket=None):
        super().__init__()

        self.option = option
        if not isinstance(self.option, Option):
            raise ValueError('option should be an Option object')

        self.state = EventConstant.DISCONNECTED

        self.retryCounter = 0

        self.socket = socket if isinstance(socket, MyWebSocket) \
            else MyWebSocket(self.option.url)

        def socket_open_callback(_):
            """
            callback function of socket open event
            :param _: websock object
            :return: None
            """
            logger.debug('Connected to %s' % self.option.url)
            self.update_state(EventConstant.CONNECTED)
        self.socket.on_open = socket_open_callback

        def socket_close_callback(_):
            """
            callback function of socket close event
            :param _: websock object
            :return: None
            """
            if self.state != EventConstant.DISCONNECTING:
                self.retryCounter += 1
                logger.error('Connection lost. Attempting to reconnect')
                self.update_state(EventConstant.DISCONNECTED)
                if self.retryCounter <= 10:
                    self.connect()
                else:
                    logger.error(
                        'Reconnect failed for 10 times. Stop reconnecting')
            else:
                self.update_state(EventConstant.DISCONNECTED)
        self.socket.on_close = socket_close_callback

        def socket_message_callback(_, orig_msg):
            """
            callback function of socket message event
            :param _: websock object
            :param orig_msg: message in json format
            :return:
            """
            try:
                msg = Response.deserialize(orig_msg)
                self.emit(msg.get_response_name(), msg)
                logger.info('get %s response' % (msg.get_response_name()))
            except Exception as e:
                self.emit(EventConstant.ERROR, e)
        self.socket.on_message = socket_message_callback

        def socket_error_callback(_, error):
            """
            callback function of socket error event
            :param _: websocket object
            :param error: error
            :return: None
            """
            self.emit(EventConstant.ERROR, error)
        self.socket.on_error = socket_error_callback

        if option.auto_connect is True:
            self.connect()

    def update_state(self, state):
        """
        update the state of connection
        :param state:
        :return:
        """
        self.state = state
        logger.debug('Connection state:%s' % self.state)
        print('Client state : %s' % self.state)
        self.emit(self.state)

    def connect(self):
        """
        connect to server in websocket protocol
        :return:
        """

        if self.retryCounter > 10:
            self.retryCounter = 0

        if self.state == EventConstant.CONNECTING:
            raise ConnectionErr('Already connecting')

        elif self.state == EventConstant.CONNECTED:
            raise ConnectionErr('Already connected')

        self.update_state(EventConstant.CONNECTING)

        def run():
            """
            thread running in backend for handle the message received from server
            :return:
            """
            try:
                self.socket.run_forever()
            except Exception as e:
                self.emit(EventConstant.ERROR, e)
        thread = threading.Thread(target=run)
        thread.start()

    def disconnect(self):
        """
        disconnect the websocket connection
        :return:
        """
        if self.state == EventConstant.DISCONNECTING:
            raise ConnectionErr('Already disconnecting')
        elif self.state == EventConstant.DISCONNECTED:
            raise ConnectionErr('Already disconnected')

        self.update_state(EventConstant.DISCONNECTING)
        self.socket.close()

    def send(self, request):
        """
        send request to server by websocket
        :param request:
        :return:
        """
        try:
            self.socket.send(request.serialize())
        except Exception as e:
            self.emit(EventConstant.ERROR, e)
