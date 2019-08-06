"""

This module provides Client object
"""


from collections import defaultdict
import logging
import copy
import threading

from streamr.client.event import Event
from streamr.client.connection import Connection
from streamr.client.subscription import Subscription
from streamr.util.option import Option
from streamr.util.constant import EventConstant
from streamr.client.errors.error import ConnectionErr
from streamr.rest.session import get_session_token_by_api_key
from streamr.rest.stream import creating, getting_by_name, getting_by_id
from streamr.protocol.request import PublishRequest, ResendRequest, SubscribeRequest, UnsubscribeRequest
from streamr.protocol.errors.error import InvalidJsonError

__all___ = ['Client']

logger = logging.getLogger(__name__)


class Client(Event):
    """
    Client class
    """

    def __init__(self, option, connection=None):

        super().__init__()

        self.subs_by_stream_id = defaultdict(list)
        self.sub_by_sub_id = {}
        self.publish_queue = []

        if not isinstance(option, Option):
            raise ValueError('First parameter should be an Option object.')

        self.option = option

        self.option.check_api()
        self.option.check_url()

        self.session_thread = None
        self.session_token = None
        self.session_thread_lock = threading.Lock()
        self.__auto_update_session_token()

        if self.session_token is None:
            self._close_session_thread()
            raise ConnectionErr('Get session token failed')

        self.connection = connection if connection is not None else Connection(self.option)

        def broadcast_msg(msg):
            """
            callback function of broadcast response
            :param msg: msg received from server
            :return: None
            """
            subs = self.subs_by_stream_id.get(msg.payload.stream_id, None)
            if subs is not None:
                for sub in subs:
                    sub.handle_message(msg.payload, False)
            else:
                logger.debug(
                    'WARN: message received for stream with no subscriptions: %s' % msg.stream_id)
        self.connection.on('BroadcastMessage', broadcast_msg)

        def unicast_msg(msg):
            """
            callback function of unicast response
            :param msg: msg received from server
            :return: None
            """
            if msg.sub_id is not None and self.sub_by_sub_id.get(msg.sub_id, None) is not None:
                self.sub_by_sub_id[msg.sub_id].handle_message(msg.payload, True)
            else:
                logger.debug('WARN: subscription not found for stream: %s, sub: %s' % (
                    msg.stream_id, msg.sub_id))
        self.connection.on('UnicastMessage', unicast_msg)

        def subscribe_response(msg):
            """
            callback function of subscribe response
            :param msg: msg received from server
            :return: None
            """
            subs = self.subs_by_stream_id.get(msg.payload.stream_id, [])
            if len(subs) != 0:
                for sub in subs:
                    if sub.resending is False:
                        sub.set_state(EventConstant.SUBSCRIBED)
            logger.debug('Client subscribed :%s' % msg.payload)
        self.connection.on('SubscribeResponse', subscribe_response)

        def unsubscribe_response(msg):
            """
            callback function of unsubscribe response
            :param msg: msg received from server
            :return: None
            """
            subs = self.subs_by_stream_id.get(msg.payload.stream_id, [])
            if len(subs) != 0:
                for sub in subs:
                    self.__remove_subscription(sub)
                    sub.set_state(EventConstant.UNSUBSCRIBED)
            self.__check_auto_disconnect()
            logger.debug('Client unsubscribed :%s' % msg.payload)
        self.connection.on('UnsubscribeResponse', unsubscribe_response)

        def resend_response_resending(msg):
            """
            callback function of resendResponseResending
            :param msg: msg received from server
            :return: None
            """
            if self.sub_by_sub_id.get(msg.payload.sub_id, None) is not None:
                self.sub_by_sub_id[msg.payload.sub_id].emit(EventConstant.RESENDING, msg.payload)
            else:
                logger.debug('resent: Subscription %s is gone already' %
                             msg.payload.sub_id)
        self.connection.on('ResendResponseResending', resend_response_resending)

        def resend_response_resent(msg):
            """
            callback function of resendResponseResent
            :param msg: msg received from server
            :return: None
            """
            if self.sub_by_sub_id.get(msg.payload.sub_id, None) is not None:
                self.sub_by_sub_id[msg.payload.sub_id].emit(EventConstant.RESENT, msg.payload)
            else:
                logger.debug(
                    'resent: Subscription %s is gone already', msg.payload.sub_id)
        self.connection.on('ResendResponseResent', resend_response_resent)

        def resend_response_no_resend(msg):
            """
            callback function of resendResponseNoResent
            :param msg: msg received from server
            :return: None
            """
            if self.sub_by_sub_id.get(msg.payload.sub_id, None) is not None:
                self.sub_by_sub_id[msg.payload.sub_id].emit(EventConstant.NO_RESEND, msg.payload)
            else:
                logger.debug('resent: Subscription %s is gone already' %
                             msg.payload.sub_id)
        self.connection.on('ResendResponseNoResend', resend_response_no_resend)

        def connected_listener():
            """
            callback function of connected event
            :return: None
            """
            logger.debug('Connected')
            for key in self.subs_by_stream_id.keys():
                subs = self.subs_by_stream_id[key]
                for sub in subs:
                    if sub.get_state() != EventConstant.SUBSCRIBED:
                        self.__resend_and_subscribe(sub)

            publish_queue_copy = self.publish_queue
            self.publish_queue = []
            for element in publish_queue_copy:
                self.publish(*element)
            self.emit(EventConstant.CONNECTED)
        self.connection.on(EventConstant.CONNECTED, connected_listener)

        def disconnected_listener():
            """
            callback function of disconnected event
            :return: None
            """
            logger.debug('Disconnected')
            for k in self.subs_by_stream_id.keys():
                subs = self.subs_by_stream_id[k]
                for sub in subs:
                    sub.set_state(EventConstant.UNSUBSCRIBED)
            self.emit(EventConstant.DISCONNECTED)
        self.connection.on(EventConstant.DISCONNECTED, disconnected_listener)

        def error(err):
            """
            callback function of error event
            :param err: error object
            :return: None
            """
            if isinstance(err, InvalidJsonError):
                subs = self.subs_by_stream_id[err.stream_id]
                for sub in subs:
                    sub.handle_error(err)
            else:
                logger.error(err.with_traceback(err.__traceback__))
        self.connection.on(EventConstant.ERROR, error)

    def __auto_update_session_token(self):
        self.session_thread_lock.acquire()
        self.session_token = get_session_token_by_api_key(self.option.api_key)
        t = threading.Timer(self.option.session_token_refresh_interval, self.__auto_update_session_token)
        t.start()
        self.session_thread = t
        self.session_thread_lock.release()

    def __add_subscription(self, sub):
        self.sub_by_sub_id[sub.sub_id] = sub
        self.subs_by_stream_id[sub.stream_id].append(sub)

    def __remove_subscription(self, sub):
        if sub.sub_id in self.sub_by_sub_id.keys():
            self.sub_by_sub_id.pop(sub.sub_id)
        subs = self.subs_by_stream_id.get(sub.stream_id, [])
        if len(subs) != 0:
            for i in sorted(range(len(subs)), reverse=True):
                if subs[i].sub_id == sub.sub_id:
                    subs.pop(i)
            self.subs_by_stream_id[sub.stream_id] = subs

    def publish(self, object_or_id, data, api_key=None):
        """
        publish function
        :param object_or_id: str contains streamId or a object contains a attribute of stream_id
        :param data: data need be published
        :param api_key: api_key
        :return: None
        """
        if hasattr(object_or_id, 'stream_id'):
            stream_id = object_or_id.stream_id
        elif isinstance(object_or_id, str):
            stream_id = object_or_id
        else:
            raise ValueError('stream_id only support str or objects containing an stream_id attribute')

        if api_key is None:
            api_key = self.option.api_key

        if not isinstance(data, (list, dict)):
            raise ValueError('data must be an dict or list ! Given: %s' % (type(data)))

        if self.is_connected() is True:
            self.__request_publish(stream_id, data, api_key)
        elif self.option.auto_connect is True:
            self.publish_queue.append([stream_id, data, api_key])
            try:
                self.connect()
            except Exception as e:
                raise ConnectionErr(e)
        else:
            raise ConnectionErr(
                'Wait for connected event before calling publish or set autoConnect to True')

    def connect(self):
        """
        connect to server for publishing data
        :return: None
        """
        if self.is_connected() is True:
            raise ConnectionErr('Already connected!')
        elif self.connection.state == EventConstant.CONNECTING:
            raise ConnectionErr('Already connecting')

        logger.debug('Connecting to %s' % self.option.url)
        if self.session_thread is None or self.session_thread.is_alive() is False:
            self.__auto_update_session_token()
        self.connection.connect()

    def subscribe(self, stream, callback, legacy_option=None):
        """
        subscribe to stream with given id
        :param stream: object or dict contains stream_id and stream_partition
        :param callback: callback function when subscribed
        :param legacy_option: backward compatibility
        :return: subscription
        """
        if hasattr(stream, 'stream_id'):
            stream_id = stream.stream_id
        elif isinstance(stream, dict) and ('stream_id' in stream.keys() or 'stream' in stream.keys()):
            stream_id = stream.get('stream', None) or stream.get('stream_id', None)
        elif isinstance(stream, str):
            stream_id = stream
        else:
            raise ValueError('subscribe: stream_id and stream_partition should be given. Given :%s' % (
                type(stream)))

        if isinstance(legacy_option, Option):
            opt = copy.copy(legacy_option)
            opt.stream_id = stream_id
        else:
            opt = Option()

        sub = Subscription(stream_id, opt.stream_partition or 0, self.option.api_key, callback, opt)

        def gap_handler(from_, to_):
            """
            handler when subscription detect a gap
            :param from_:
            :param to_:
            :return: None
            """
            if not sub.resending:
                self.__request_resend(
                    sub, Option(resend_from=from_, resend_to=to_))
        sub.on(EventConstant.GAP, gap_handler)

        def done_handler():
            """
            handler when subscription is done
            :return: None
            """
            logger.debug('done event for sub %s ' % sub.sub_id)
            self.unsubscribe(sub)
        sub.on(EventConstant.DONE, done_handler)

        self.__add_subscription(sub)

        if self.connection.state == EventConstant.CONNECTED:
            self.__resend_and_subscribe(sub)
        elif self.option.auto_connect is True:
            try:
                self.connect()
            except Exception as e:
                import traceback
                traceback.print_exc()
                raise ConnectionErr(e)

        return sub

    def unsubscribe(self, sub):
        """
        unsubscribe stream
        :param sub: subscription
        :return: None
        """
        if sub is None or not isinstance(sub, Subscription):
            raise ValueError('unsubscribe: please give a subscription object as an argument')

        if self.subs_by_stream_id.get(sub.stream_id, None) is not None and \
                len(self.subs_by_stream_id.get(sub.stream_id, None)) == 1 and \
                self.is_connected() is True and sub.get_state() == EventConstant.SUBSCRIBED:
            sub.set_state(EventConstant.UNSUBSCRIBING)
            self.__request_unsubscribe(sub.stream_id)

        elif sub.get_state() != EventConstant.UNSUBSCRIBING and \
                sub.get_state() != EventConstant.UNSUBSCRIBED:
            self.__remove_subscription(sub)
            sub.set_state(EventConstant.UNSUBSCRIBED)
            self.__check_auto_disconnect()

    def unsubscribe_all(self, stream_id):
        """
        unsubscribe stream_id
        :param stream_id:
        :return: None
        """
        if not isinstance(stream_id, str):
            raise ValueError('unsubscribe: stream sub_id must be a string')

        if self.subs_by_stream_id.get(stream_id, None) is not None:
            subs = copy.copy(self.subs_by_stream_id[stream_id])
            for sub in subs:
                self.unsubscribe(sub)

    def is_connected(self):
        """
        :return: the connection state
        """
        return self.connection.state == EventConstant.CONNECTED

    def reconnected(self):
        """
        reconnect to server
        :return: None
        """
        self.connect()

    def pause(self):
        """
        disconnect from server
        :return: None
        """
        self.connection.disconnect()

    def _close_session_thread(self):
        """
        shutdown session_thread
        :return: None
        """
        self.session_thread_lock.acquire()
        self.session_thread.cancel()
        self.session_thread_lock.release()

    def disconnect(self):
        """
        disconnect from server
        :return: None
        """
        self.subs_by_stream_id = defaultdict(list)
        self.sub_by_sub_id = {}
        self.connection.disconnect()
        self._close_session_thread()

    def __check_auto_disconnect(self):
        if self.option.auto_disconnect is True and len(self.subs_by_stream_id.keys()) == 0:
            logger.debug(
                'Disconnecting due to no longer being subscribed to any streams')
            self.disconnect()

    def __resend_and_subscribe(self, sub):
        if sub.get_state() != EventConstant.SUBSCRIBED and not sub.resending:

            def subscribed():
                """
                callback function of subscribed event
                :return:
                """
                if sub.has_resend_option():
                    self.__request_resend(sub)
            sub.once(EventConstant.SUBSCRIBED, subscribed)

            self.__request_subscribe(sub)

    def __request_subscribe(self, sub):

        subs = self.subs_by_stream_id.get(sub.stream_id, [])

        subscribed_subs = []
        for s in subs:
            if s.get_state() == EventConstant.SUBSCRIBED:
                subscribed_subs.append(s)

        if len(subscribed_subs) == 0:
            request = SubscribeRequest(
                sub.stream_id, 0, sub.api_key, self.session_token)
            logger.debug('_requestSubscribing client :%s' % request)
            self.connection.send(request)
            sub.set_state(EventConstant.SUBSCRIBING)
        elif len(subscribed_subs) > 0:
            logger.debug(
                '__request_subscribe: another subscription for same stream : %s, subscribing' % sub.stream_id)
            sub.set_state(EventConstant.SUBSCRIBED)

    def __request_unsubscribe(self, stream_id, partition=0, api_key=None):
        logger.debug('Client unsubscribing stream %s' % stream_id)
        self.connection.send(UnsubscribeRequest(
            stream_id, partition, api_key if api_key is not None else self.option.api_key, self.session_token))

    def __request_resend(self, sub, resend_option=None):
        sub.set_resending(True)
        request = ResendRequest(sub.stream_id, sub.stream_partition, sub.sub_id,
                                resend_option if isinstance(resend_option, Option)
                                else sub.get_effective_resend_option(),
                                sub.api_key, self.session_token)
        logger.debug('__request_resend :%s' % request)
        self.connection.send(request)

    def __request_publish(self, stream_id, data, api_key):
        request = PublishRequest(stream_id, api_key, self.session_token, data)
        logger.debug('__request_publish :%s' % request)
        self.connection.send(request)

    def handle_error(self, msg):
        """
        error handler
        :param msg: msg
        :return: None
        """
        logger.debug(msg)
        self.emit(EventConstant.ERROR, msg)

    def create_stream(self, stream_name, stream_des=None):
        """
        create stream by given stream_name
        :param stream_name:
        :param stream_des:
        :return: stream dict
        """
        stream = creating(stream_name, stream_des, self.session_token)
        return stream

    def get_stream_by_name(self, stream_name):
        """
        get stream by stream_name
        :param stream_name: str
        :return: stream list
        """
        return getting_by_name(stream_name, self.session_token)

    def get_stream_by_id(self, stream_id):
        """
        get stream by stream id
        :param stream_id: str
        :return: stream dict
        """
        return getting_by_id(stream_id, self.session_token)

    def get_or_create_stream(self, stream_name):
        """
        create stream or get a stream
        :param stream_name: str
        :return: stream list
        """
        stream = self.get_stream_by_name(stream_name)
        if stream is None:
            logger.error('stream: %s is not existed, creating...' % stream_name)
            return [self.create_stream(stream_name)]
        else:
            return stream
