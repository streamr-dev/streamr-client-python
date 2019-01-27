from streamr.client.event import Event
import logging
from streamr.protocol.errors.error import *
import json

subId = 0

__all__ = ['Subscription']

logger = logging.getLogger(__name__)

def generateSubscriptionId():
    global subId
    subId += 1
    return str(subId)

class Subscription(Event):
    
    class State:
        unsubscribed = 'unsubscribed'
        subscribing = 'subscribing'
        subscribed = 'subscribed'
        unsubscribing = 'unsubscribing'

    def __init__(self,streamId=None,streamPartition=0,apiKey=None,callback=lambda x,y:None,options={}):
        super().__init__()

        if streamId == None:
            raise Exception('No stream id givsen!')
        if callback == None:
            raise Exception('No callback given')

        self.id = generateSubscriptionId()
        self.streamId = streamId
        self.streamPartition = streamPartition
        self.apiKey = apiKey
        self.callback = callback
        self.options = options
        self.queue = []
        self.state = Subscription.State.unsubscribed
        self.resending = False
        self.lastReceivedOffset = None

        resendOptionCount = 0
        if self.options.get('resend_all',None) != None:
            resendOptionCount += 1
        if self.options.get('resend_from',None) != None:
            resendOptionCount += 1
        if self.options.get('resend_last',None) != None:
            resendOptionCount += 1
        if self.options.get('resend_from_time',None) != None:
            resendOptionCount += 1
        if resendOptionCount > 1:
            raise Exception('Multiple resend options active! Please use only one: %s'%(json.dumps(options)))

        if self.options.get('resend_from_time',None) != None and type(self.options.get('resend_from_time',None)) not in [int,float]:
            raise Exception('"resend_from_time option" must be a int or float')


        def unsubscribed():
            self.setResending(False)
        self.on('unsubscribed', unsubscribed)

        def no_resend(response=None):
            logger.debug('Sub %s no_resend:%s'%(self.id,response))
            self.setResending(False)
            self.checkQueue()
        self.on('no_resend',no_resend)

        def resent(response=None):
            logger.debug('Sub %s resent: %s'%(self.id,response))
            self.setResending(False)
            self.checkQueue()
        self.on('resent',resent)
        
        def connected():
            pass
        self.on('connected',connected)
        
        def disconnected():
            self.setState(Subscription.State.unsubscribed)
            self.setResending(False)
        self.on('disconnected',disconnected)
        
    def checkForGap(self,previousOffset):
        return previousOffset != None and self.lastReceivedOffset != None and previousOffset > self.lastReceivedOffset
        

    def handleMessage(self,msg,isResend = False):
        
        if msg.previousOffset == None:
            logger.debug('handleMessage: prevOffset is null, gap detection is impossible! message no %s'%(msg)) 
        
        if self.resending and not isResend:
            self.queue.append(msg)
        elif self.checkForGap(msg.previousOffset)  and not self.resending:

            self.queue.append(msg)
            
            from_index = self.lastReceivedOffset +1
            to_index = msg.previousOffset
            logger.debug('Gap detected, requesting resend for stream %s from %d to %d'%( self.streamId, from_index, to_index))
            self.emit('gap', from_index, to_index)
        elif self.lastReceivedOffset != None and msg.offset <= self.lastReceivedOffset:
            logger.debug('Sub %s already recevied message: %s, lastReceivedOffset :%s. Ignoring message.'%(self.id,msg.offset,self.lastReceivedOffset))
        else:
            self.lastReceivedOffset = msg.offset
            self.callback(msg.getParsedContent(),msg)
            if msg.isByeMessage():
                self.emit('done')

    def checkQueue(self):
        logger.debug('Attempting to process %s queued messages for stream %s'%(len(self.queue),self.streamId))
        
        orig = self.queue
        self.queue = []
        
        for msg in orig:
            self.handleMessage(msg,False)

    def hasResendOptions(self):
        return self.options.get('resend_all',False) == True or self.options.get('resend_from',-1) >= 0 or self.options.get('resend_from_time',-1) >=0 or self.options.get('resend_last',-1 )> 0


    def getEffectiveResendOptions(self):
        if self.hasReceviedMessage() and self.hasResendOptions() and (self.options.get('resend_all',None) or self.options.get('resend_from',None) or self.options.get('resend_from_time',None)):
            return {'resend_from':self.lastReceivedOffset + 1}

        result = {}

        for k in self.options.keys():
            if str.startswith(k, 'resend_'):
                result[k] = self.options[k]

        return result


    def hasReceviedMessage(self):
        return self.lastReceivedOffset != None

    def getState(self):
        return self.state

    def setState(self,state):
        logger.debug('Subscription: stream %s state changed %s => %s'%(self.streamId,self.state,state))
        self.state = state
        self.emit(state)

    def isResending(self):
        return self.resending

    def setResending(self,v):
        logger.debug('Subscription: Stream %s resending: %s'%(self.streamId,v) )
        self.resending = v


    def handleError(self,err):
        if isinstance(err,InvalidJsonError) and not self.checkForGap(err.streamMessage.previousOffset):
            self.lastReceivedOffset = err.streamMessage.offset

        self.emit('error',err)
