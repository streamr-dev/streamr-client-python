"""
 modified from  websock-client _app module
 we change the close function
 delete "raise WebSocketException("socket is already opened")" for reconnecting to server
 delete Dispacher class
 reformat the sequence of code
"""


import inspect
import select
import sys
import time
import traceback
import logging

import six

from websocket._abnf import ABNF
from websocket._core import WebSocket, getdefaulttimeout
from websocket._exceptions import *
from websocket import _logging

'''
modified from websock.WebsocketApp
'''
__all__ = ["MyWebSocket"]

logger = logging.getLogger(__name__)


class MyWebSocket(object):
    """
    Higher level of APIs are provided.
    The interface is like JavaScript WebSocket object.
    """

    def __init__(self, url, header=None,
                 on_open=None, on_message=None, on_error=None,
                 on_close=None, on_ping=None, on_pong=None,
                 on_cont_message=None,
                 get_mask_key=None, cookie=None,
                 subprotocols=None,
                 on_data=None):

        """
        url: websocket url.
        header: custom header for websocket handshake.
        on_open: callable object which is called at opening websocket.
          this function has one argument. The argument is this class object.
        on_message: callable object which is called when received data.
         on_message has 2 arguments.
         The 1st argument is this class object.
         The 2nd argument is utf-8 string which we get from the server.
        on_error: callable object which is called when we get error.
         on_error has 2 arguments.
         The 1st argument is this class object.
         The 2nd argument is exception object.
        on_close: callable object which is called when closed the connection.
         this function has one argument. The argument is this class object.
        on_cont_message: callback object which is called when receive continued
         frame data.
         on_cont_message has 3 arguments.
         The 1st argument is this class object.
         The 2nd argument is utf-8 string which we get from the server.
         The 3rd argument is continue flag. if 0, the data continue
         to next frame data
        on_data: callback object which is called when a message received.
          This is called before on_message or on_cont_message,
          and then on_message or on_cont_message is called.
          on_data has 4 argument.
          The 1st argument is this class object.
          The 2nd argument is utf-8 string which we get from the server.
          The 3rd argument is data type. ABNF.OPCODE_TEXT or ABNF.OPCODE_BINARY will be came.
          The 4th argument is continue flag. if 0, the data continue
        keep_running: this parameter is obsolete and ignored.
        get_mask_key: a callable to produce new mask keys,
          see the WebSocket.set_mask_key's docstring for more information
        subprotocols: array of available sub protocols. default is None.
        """
        self.url = url
        self.header = header if header is not None else []
        self.cookie = cookie
        self.on_open = on_open
        self.on_message = on_message
        self.on_data = on_data
        self.on_error = on_error
        self.on_close = on_close
        self.on_ping = on_ping
        self.on_pong = on_pong
        self.on_cont_message = on_cont_message
        self.keep_running = True
        self.get_mask_key = get_mask_key
        self.sock = None
        self.subprotocols = subprotocols
        self.frame = None

    def send(self, data, opcode=ABNF.OPCODE_TEXT):
        """
        send message.
        data: message to send. If you set opcode to OPCODE_TEXT,
              data must be utf-8 string or unicode.
        opcode: operation code of data. default is OPCODE_TEXT.
        """

        if not self.sock or self.sock.send(data, opcode) == 0:
            raise WebSocketConnectionClosedException("Connection is already closed.")

    def close(self):
        """
        our websock are running in a thread
        we use keep_running flg to shutdown the connection
        """
        self.keep_running = False

    def run_forever(self, sockopt=None, sslopt=None,
                    ping_interval=0,
                    http_proxy_host=None, http_proxy_port=None,
                    http_no_proxy=None, http_proxy_auth=None,
                    skip_utf8_validation=False,
                    host=None, origin=None,
                    suppress_origin=False, proxy_type=None):
        """
        run event loop for WebSocket framework.
        This loop is infinite loop and is alive during websocket is available.
        sockopt: values for socket.setsockopt.
            sockopt must be tuple
            and each element is argument of sock.setsockopt.
        sslopt: ssl socket optional dict.
        ping_interval: automatically send "ping" command
            every specified period(second)
            if set to 0, not send automatically.
        ping_timeout: timeout(second) if the pong message is not received.
        http_proxy_host: http proxy host name.
        http_proxy_port: http proxy port. If not set, set to 80.
        http_no_proxy: host names, which doesn't use proxy.
        skip_utf8_validation: skip utf8 validation.
        host: update host header.
        origin: update origin header.
        dispatcher: customize reading data from socket.
        suppress_origin: suppress outputting origin header.

        Returns
        -------
        False if caught KeyboardInterrupt
        True if other exception was raised during a loop
        """
        self.keep_running = True

        if sockopt is None:
            sockopt = list()

        if sslopt is None:
            sslopt = dict()

        def teardown(close_frame=None):
            """
            Tears down the connection.
            If close_frame is set, we will invoke the on_close handler with the
            statusCode and reason from there.
            """
            self.frame = None
            self.keep_running = False
            if self.sock:
                self.sock.close()
                self.sock = None
            close_args = self._get_close_args(
                close_frame.data if close_frame else None)
            self._callback(self.on_close, *close_args)

        def read():
            """
            receive data from socket
            :return:
            """
            op_code, frame = self.sock.recv_data_frame(True)

            if op_code == ABNF.OPCODE_CLOSE:
                self.keep_running = False
                self.frame = frame
            elif op_code == ABNF.OPCODE_PING:
                self._callback(self.on_ping, frame.data)
            elif op_code == ABNF.OPCODE_PONG:
                self.last_pong_tm = time.time()
                self._callback(self.on_pong, frame.data)
            elif op_code == ABNF.OPCODE_CONT and self.on_cont_message:
                self._callback(self.on_data, frame.data,
                               frame.opcode, frame.fin)
                self._callback(self.on_cont_message,
                               frame.data, frame.fin)
            else:
                data = frame.data
                if six.PY3 and op_code == ABNF.OPCODE_TEXT:
                    data = data.decode("utf-8")
                self._callback(self.on_data, data, frame.opcode, True)
                self._callback(self.on_message, data)

        try:
            self.sock = WebSocket(
                self.get_mask_key, sockopt=sockopt, sslopt=sslopt,
                fire_cont_frame=self.on_cont_message is not None,
                skip_utf8_validation=skip_utf8_validation,
                enable_multithread=True if ping_interval else False)
            self.sock.settimeout(getdefaulttimeout())
            self.sock.connect(
                self.url, header=self.header, cookie=self.cookie,
                http_proxy_host=http_proxy_host,
                http_proxy_port=http_proxy_port, http_no_proxy=http_no_proxy,
                http_proxy_auth=http_proxy_auth, subprotocols=self.subprotocols,
                host=host, origin=origin, suppress_origin=suppress_origin,
                proxy_type=proxy_type)

            self._callback(self.on_open)

            while self.sock.connected and self.keep_running:
                r, w, e = select.select((self.sock.sock,), (), (), 1)
                if r:
                    read()
            teardown(self.frame)
        except (Exception, KeyboardInterrupt, SystemExit) as e:
            self._callback(self.on_error, e)
            teardown()

    def _get_close_args(self, data):
        """ this functions extracts the code, reason from the close body
        if they exists, and if the self.on_close except three arguments """
        # if the on_close callback is "old", just return empty list
        if sys.version_info < (3, 0):
            if not self.on_close or len(inspect.getfullargspec(self.on_close).args) != 3:
                return []
        else:
            if not self.on_close or len(inspect.getfullargspec(self.on_close).args) != 3:
                return []

        if data and len(data) >= 2:
            code = 256 * six.byte2int(data[0:1]) + six.byte2int(data[1:2])
            reason = data[2:].decode('utf-8')
            return [code, reason]

        return [None, None]

    def _callback(self, callback, *args):
        if callback:
            try:
                if inspect.ismethod(callback):
                    callback(*args)
                else:
                    callback(self, *args)

            except Exception as e:
                _logging.error(
                    "error from callback {}: {}".format(callback, e))
                if _logging.isEnabledForDebug():
                    _, _, tb = sys.exc_info()
                    traceback.print_tb(tb)
