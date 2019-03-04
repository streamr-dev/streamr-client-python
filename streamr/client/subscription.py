import logging
import json

from streamr.client.event import Event
from streamr.client.errors.error import ParameterError
from streamr.protocol.errors.error import InvalidJsonError


__all__ = ['Subscription']


logger = logging.getLogger(__name__)


subId = 0


def generateSubscriptionId():
    global subId
    subId += 1
    return str(subId)


class Subscription(Event):

    class State:
        SUBSCRIBING = 'SUBSCRIBING'
        SUBSCRIBED = 'SUBSCRIBED'
        UNSUBSCRIBING = 'UNSUBSCRIBNG'
        UNSUBSCRIBED = 'UNSUBSCRIBED'

    def __init__(self, streamId=None, streamPartition=0, apiKey=None, callback=lambda x, y: None, options={}):
        super().__init__()

        if streamId is None:
            raise ParameterError('No stream id given!')
        if callback is None:
            raise ParameterError('No callback given')

        self.id = generateSubscriptionId()
        self.streamId = streamId
        self.streamPartition = streamPartition
        self.apiKey = apiKey
        self.callback = callback
        self.options = options
        self.queue = []
        self.state = Subscription.State.UNSUBSCRIBED
        self.resending = False
        self.lastReceivedOffset = None

        resendOptionCount = 0
        if self.options.get('resend_all', None) is not None:
            resendOptionCount += 1
        if self.options.get('resend_from', None) is not None:
            resendOptionCount += 1
        if self.options.get('resend_last', None) is not None:
            resendOptionCount += 1
        if self.options.get('resend_from_time', None) is not None:
            resendOptionCount += 1
        if resendOptionCount > 1:
            raise ParameterError('Multiple resend options active! Please use only one: %s' % (
                json.dumps(options)))

        if self.options.get('resend_from_time', None) is not None:
            t = self.options.get('resend_from_time', None)
            if not isinstance(t, (int, float)):
                raise ParameterError(
                    '"resend_from_time option" must be an int or float')

        def unsubscribed():
            self.setResending(False)
        self.on('unsubscribed', unsubscribed)

        def no_resend(response=None):
            logger.debug('Sub %s no_resend:%s' % (self.id, response))
            self.setResending(False)
            self.checkQueue()
        self.on('no_resend', no_resend)

        def resent(response=None):
            logger.debug('Sub %s resent: %s' % (self.id, response))
            self.setResending(False)
            self.checkQueue()
        self.on('resent', resent)

        def connected():
            pass
        self.on('connected', connected)

        def disconnected():
            self.setState(Subscription.State.UNSUBSCRIBED)
            self.setResending(False)
        self.on('disconnected', disconnected)

    def checkForGap(self, previousOffset):
        return previousOffset is not None and self.lastReceivedOffset is not None and previousOffset > self.lastReceivedOffset

    def handleMessage(self, msg, isResend=False):

        if msg.previousOffset is None:
            logger.debug(
                'handleMessage: prevOffset is null, gap detection is impossible! message no %s' % (msg))

        if self.resending is True and isResend is False:
            self.queue.append(msg)
        elif self.checkForGap(msg.previousOffset) is True and self.resending is False:

            self.queue.append(msg)

            from_index = self.lastReceivedOffset + 1
            to_index = msg.previousOffset
            logger.debug('Gap detected, requesting resend for stream %s from %d to %d' % (
                self.streamId, from_index, to_index))
            self.emit('gap', from_index, to_index)
        elif self.lastReceivedOffset is not None and msg.offset <= self.lastReceivedOffset:
            logger.debug('Sub %s already recevied message: %s, lastReceivedOffset :%s. Ignoring message.' % (
                self.id, msg.offset, self.lastReceivedOffset))
        else:
            self.lastReceivedOffset = msg.offset
            self.callback(msg.getParsedContent(), msg)
            if msg.isByeMessage():
                self.emit('done')

    def checkQueue(self):
        logger.debug('Attempting to process %s queued messages for stream %s' % (
            len(self.queue), self.streamId))

        orig = self.queue
        self.queue = []

        for msg in orig:
            self.handleMessage(msg, False)

    def hasResendOptions(self):
        return self.options.get('resend_all', False) == True or self.options.get('resend_from', -1) >= 0 or self.options.get('resend_from_time', -1) >= 0 or self.options.get('resend_last', -1) > 0

    def getEffectiveResendOptions(self):
        if self.hasReceviedMessage() and self.hasResendOptions() and (self.options.get('resend_all', None) is not None or self.options.get('resend_from', None) is not None or self.options.get('resend_from_time', None) is not None):
            return {'resend_from': self.lastReceivedOffset + 1}
        result = {}

        for k in self.options.keys():
            if str.startswith(k, 'resend_'):
                result[k] = self.options[k]

        return result

    def hasReceviedMessage(self):
        return self.lastReceivedOffset is not None

    def getState(self):
        return self.state

    def setState(self, state):
        logger.debug('Subscription: stream %s state changed %s => %s' %
                     (self.streamId, self.state, state))
        self.state = state
        self.emit(state)

    def isResending(self):
        return self.resending

    def setResending(self, v):
        logger.debug('Subscription: Stream %s resending: %s' %
                     (self.streamId, v))
        self.resending = v

    def handleError(self, err):
        if isinstance(err, InvalidJsonError) and not self.checkForGap(err.streamMessage.previousOffset):
            self.lastReceivedOffset = err.streamMessage.offset

        self.emit('error', err)
