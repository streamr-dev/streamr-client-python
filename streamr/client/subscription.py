"""
provide the subscription class
"""

import logging

from streamr.client.event import Event
from streamr.util.option import Option
from streamr.util.constant import EventConstant
from streamr.protocol.errors.error import InvalidJsonError

__all__ = ['Subscription']

logger = logging.getLogger(__name__)

SUB_ID_COUNTS = 0


def generate_subscription_id():
    """
    generate a unique id for subscription
    :return:
    """
    global SUB_ID_COUNTS
    SUB_ID_COUNTS += 1
    return str(SUB_ID_COUNTS)


class Subscription(Event):
    """
    subscription class
    """

    def __init__(self, stream_id=None, stream_partition=0, api_key=None, callback=None, option=None):
        super().__init__()

        if stream_id is None:
            raise ValueError('No stream_id given!')
        if api_key is None:
            raise ValueError('No api_key given!')
        if not hasattr(callback, '__call__'):
            raise ValueError('No callback given')

        self.sub_id = generate_subscription_id()
        self.stream_id = stream_id
        self.stream_partition = stream_partition
        self.api_key = api_key
        self.callback = callback if hasattr(callback, '__call__') else lambda x, y: None
        if isinstance(option, Option):
            self.option = option
        else:
            self.option = Option()
        self.queue = []
        self.state = EventConstant.UNSUBSCRIBED
        self.resending = False
        self.last_received_offset = None

        if self.option.check_resend() > 1:
            raise ValueError('Multiple resend option active! Please use only one: %s' % self.option)

        if self.option.resend_from_time is not None:
            t = self.option.resend_from_time
            if not isinstance(t, (int, float)):
                raise ValueError(
                    '"resend_from_time option" must be an int or float')

        def unsubscribed():
            """
            callback function of unsubscribed event
            :return:
            """
            self.set_resending(False)

        self.on(EventConstant.UNSUBSCRIBED, unsubscribed)

        def no_resend(response=None):
            """
            callback function of no_resend event
            :param response:
            :return:
            """
            logger.debug('Sub %s no_resend:%s' % (self.sub_id, response))
            self.set_resending(False)
            self.check_queue()

        self.on(EventConstant.NO_RESEND, no_resend)

        def resent(response=None):
            """
            callback function of resent event
            :param response:
            :return:
            """
            logger.debug('Sub %s resent: %s' % (self.sub_id, response))
            self.set_resending(False)
            self.check_queue()

        self.on(EventConstant.RESENT, resent)

        def connected():
            """
            callback function of connected event
            :return:
            """
            pass

        self.on(EventConstant.CONNECTED, connected)

        def disconnected():
            """
            callback function of disconnected event
            :return:
            """
            self.set_state(EventConstant.UNSUBSCRIBED)
            self.set_resending(False)

        self.on(EventConstant.DISCONNECTED, disconnected)

    def check_for_gap(self, previous_offset):
        """
        check whether some msg is missed
        :param previous_offset:
        :return:
        """
        return previous_offset is not None \
            and self.last_received_offset is not None \
            and previous_offset > self.last_received_offset

    def handle_message(self, msg, is_resend=False):
        """
        handle the message received from server
        :param msg:
        :param is_resend:
        :return:
        """
        if msg.previous_offset is None:
            logger.debug(
                'handle_message: prev_offset is null, gap detection is impossible! message no. %s' % msg.serialize())

        if self.resending is True and is_resend is False:
            self.queue.append(msg)
        elif self.check_for_gap(msg.previous_offset) is True and self.resending is False:

            self.queue.append(msg)

            from_index = self.last_received_offset + 1
            to_index = msg.previous_offset
            logger.debug('Gap detected, requesting resend for stream %s from %d to %d' % (
                self.stream_id, from_index, to_index))
            self.emit(EventConstant.GAP, from_index, to_index)
        elif self.last_received_offset is not None and msg.offset <= self.last_received_offset:
            logger.debug('Sub %s already received message: %s, last_received_offset :%s. Ignoring message.' % (
                self.sub_id, msg.offset, self.last_received_offset))
        else:
            self.last_received_offset = msg.offset
            self.callback(msg.get_parsed_content(), msg)
            if msg.is_bye_message():
                self.emit(EventConstant.DONE)

    def check_queue(self):
        """
        check whether there are data should be sent
        :return:
        """
        logger.debug('Attempting to process %s queued messages for stream %s' % (
            len(self.queue), self.stream_id))

        orig = self.queue
        self.queue = []

        for msg in orig:
            self.handle_message(msg, False)

    def has_resend_option(self):
        """
        check whether subscription has a resend option
        :return:
        """
        return self.option.check_resend() > 0

    def has_received_message(self):
        """
        check whether subscription has received message
        :return:
        """
        return self.last_received_offset is not None

    def get_effective_resend_option(self):
        """
        return the resend option
        :return:
        """
        if self.has_received_message() and self.has_resend_option() and \
                (self.option.resend_all is not None or
                 self.option.resend_from is not None or
                 self.option.resend_from_time is not None):
            return Option(resend_from=self.last_received_offset + 1)
        else:
            return Option(resend_from_time=self.option.resend_from_time,
                          resend_from=self.option.resend_from,
                          resend_all=self.option.resend_all,
                          resend_last=self.option.resend_last,
                          resend_to=self.option.resend_to)

    def get_state(self):
        """
        return the state of subscription
        :return:
        """
        return self.state

    def set_state(self, state):
        """
        change the state of subscription
        :param state:
        :return:
        """
        logger.debug('Subscription: stream %s state changed %s => %s' %
                     (self.stream_id, self.state, state))
        self.state = state
        self.emit(state)

    def is_resending(self):
        """
        check whether subscription is resending
        :return:
        """
        return self.resending

    def set_resending(self, v):
        """
        turn on or off the resending state
        :param v:
        :return:
        """
        logger.debug('Subscription: Stream %s resending: %s' %
                     (self.stream_id, v))
        self.resending = v

    def handle_error(self, err):
        """
        handle the error
        :param err:
        :return:
        """
        if isinstance(err, InvalidJsonError) and not self.check_for_gap(err.stream_message.previous_offset):
            self.last_received_offset = err.stream_message.offset

        self.emit(EventConstant.ERROR, err)
