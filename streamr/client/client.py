from collections import defaultdict
import logging
import copy
import threading

from streamr.client.event import Event
from streamr.client.connection import Connection
from streamr.rest.session import getSeToken_APIKey
from streamr.rest.restful import creating, getting_by_name, getting_by_id
from streamr.protocol.response import *
from streamr.protocol.payloads import *
from streamr.protocol.request import *
from streamr.client.subscription import Subscription


__all___ = ['Client']

logger = logging.getLogger(__name__)

class Client(Event):
    """
    This class constructs a client object:

    paras:
        options: dictionary 
        {"url": 'wss://'
        "apiKey": 'your-api-Key'
        }

    methods:
        createStream(streamName[,streamDescription])
            creating a new stream with given streamName

        getOrCreateStream(streamName)
            getting a stream by streamName, if not found then createStream

        getStreamByName(streamName)
            getting a stream by streamName

        getStreamById(streamId)
            getting a stream by streamId

        connect()
            connect to Websock server

        disconnect()
            disconect from Websock server

        subscribe(optionOrStreamId, callback)
            subscribe to stream
            optionOrStreamId: {'stream':streamid} or 'streamId'
            callback: callback function

        unsubscribe(subscription)
            unsubscribe a subscription

        unsubscribeAll(streamId)
            unsubscribe all subscriptions with the streamId

        publish(subscription,data[,apiKey])
            publish data to stream
            subscription: contain streamId
            data: dictionary
            apiKey: apiKey

    """

    
    def __init__(self,options,connection = None):
        super().__init__()

        self.options = {'url':'wss://www.streamr.com/api/v1/ws',
                        'restUrl':'https://www.streamr.com/api/v1',
                        'autoConnect':True,
                        'autoDisconnect':True,
                        'auth':defaultdict(dict),
                        'sessionTokenRefreshInterval':7200
                        }
        self.subsByStream = defaultdict(list)
        self.subById = {}
        self.publishQueue = []

        self.options = {**self.options,**options}
        
        if self.options.get('apiKey',None) == None and self.options.get('authKey',None) == None and self.options.get('auth',{}).get('apiKey',None) == None:
            raise Exception('apiKey is required')
        
        if self.options.get('authKey',None) != None and self.options.get('apiKey',None) == None:
            self.options['apiKey'] = self.options['authKey']
        
        if self.options.get('apiKey',None) != None:
            self.options['auth']['apiKey'] = self.options['apiKey']
        
        self.sessionThread = None
        self.__autoUpdateSessionToken()
        if self.sessionToken  == None:
            raise Exception('get sessionToken failed')

        self.connection = connection if connection != None else Connection(self.options)

        def broadcastMsg(msg):
            subs = self.subsByStream.get(msg.payload.streamId,None)
            if subs != None:
                for sub in subs:
                    sub.handleMessage(msg.payload,False)
            else:
                logger.debug('WARN: message recevied for stream with no subscriptions: %s',msg['streamId'])
        self.connection.on('BroadcastMessage',broadcastMsg)
        

        def unicastMsg(msg):
            if msg.subId != None and self.subById.get(msg.subId,None) !=None:
                self.subById[msg.subId].handleMessage(msg.payload,True)
            else:
                logger.debug('WARN: subscription not fround for stream: %s, sub: %s'%(msg['streamId'],msg['subId']))
        self.connection.on('UnicastMessage', unicastMsg)

        def subscribeResponse(msg):
            subs = self.subsByStream.get(msg.payload.streamId,[])
            if len(subs) != 0:
                for sub in subs:
                    if sub.resending == False:
                        sub.setState(Subscription.State.subscribed)
            logger.debug('Client subscribed :%s'%(msg.payload))
        self.connection.on('SubscribeResponse',subscribeResponse)
        
        def unsubscribeResponse(msg):
            subs = self.subsByStream.get(msg.payload.streamId, [])
            if len(subs) != 0 :
                for sub in subs:
                    self.__removeSubscription(sub)
                    sub.setState(Subscription.State.unsubscribed)
            self.__checkAutoDisconnect()
            logger.debug('Client unsubscribed :%s'%(msg.payload))
        self.connection.on('UnsubscribeResponse',unsubscribeResponse)

        def resendResponseResending(msg):
            if self.subById.get(msg.payload.subId,None):
                self.subById[msg.payload.subId].emit('resending',msg.payload)
            else:
                logger.debug('resent: Subscrption %s is gone already'%(msg.payload.subId))
        self.connection.on('ResendResponseResending', resendResponseResending)

        def resendResponseResent(msg):
            if self.subById.get(msg.payload.subId,None) :
                self.subById[msg.payload.subId].emit('resent', msg.payload)
            else:
                logger.debug('resent: Subscription %s is gone already', msg.payload.subId)
        self.connection.on('ResendResponseResent',resendResponseResent)

        def resendResponseNoResend(msg):
            if self.subById.get(msg.payload.subId,None):
                self.subById[msg.payload.subId].emit('no_resend',msg.payload)
            else:
                logger.debug('resent: Subscrption %s is gone already'%(msg.payload.subId))
        self.connection.on('ResendResponseNoResend', resendResponseNoResend)
    
        def connectedListener():
            logger.debug('Connected')
            for key in self.subsByStream.keys():
                subs = self.subsByStream[key]
                for sub in subs:
                    if sub.getState() != Subscription.State.subscribed:
                        self.__resendAndSubscribe(sub)

            publishQueueCopy = self.publishQueue
            self.publishQueue = []
            for element in publishQueueCopy:
                self.publish(*element)
            self.emit('connected')
        self.connection.on('connected', connectedListener)


        def disconnectedListener():
            logger.debug('Disconnected')

            for k in self.subsByStream.keys():
                subs = self.subsByStream[k]
                for sub in subs:
                    sub.setState(Subscription.State.unsubscribed)

            self.emit('disconnected')
        self.connection.on('disconnected', disconnectedListener)

        def errorResponse(err):
            errorObject = Exception(err.payload.error)
            self.emit('error',errorObject)
            logger.error(errorObject)
        self.connection.on('ErrorResponse',errorResponse)

        def error(err):
            if isinstance(err,InvalidJsonError):
                subs = self.subsByStream[err.streamId]
                for sub in subs:
                    sub.handleError(err)
            else:
                logger.error(err.with_traceback(err.__traceback__))
        self.connection.on('error',error)



    def __autoUpdateSessionToken(self):
        self.sessionToken = getSeToken_APIKey(self.options['apiKey'])
        t = threading.Timer(self.options.get('sessionTokenRefreshInterval',7200),self.__autoUpdateSessionToken)
        self.sessionThread = t
        t.start()

    def __addSubscription(self,sub):
        self.subById[sub.id] = sub
        self.subsByStream[sub.streamId].append(sub)

    def __removeSubscription(self,sub):
        if sub.id in self.subById.keys():
            self.subById.pop(sub.id)
        subs = self.subsByStream.get(sub.streamId, [])
        if  len(subs) != 0:
            for i in sorted(range(len(subs)),reverse=True):
                if subs[i].id == sub.id:
                    subs.pop(i)
            self.subsByStream[sub.streamId] = subs
    
    def publish(self,objectOrId,data,apiKey = None):
        if apiKey == None:
            apiKey = self.options['auth']['apiKey']
            
        streamId = objectOrId.streamId

        if type(data) not in [list,dict]:
            raise Exception('Message data must be an object ! Was: %s'%(type(data)))

        if self.isConnected():
            self.__requestPublish(streamId,data,apiKey)
        elif self.options.get('autoConnect',False):
            self.publishQueue.append([streamId,data,apiKey])
            try:
                self.connect()
            except:
                pass
        else:
            raise Exception('Wait for conneted event before calling publish or set autoConnect to True')

    def connect(self):
        if self.isConnected():
            raise Exception('Already connected!')
        elif self.connection.state == Connection.State.CONNECTING:
            raise Exception('Already connecting')

        logger.debug('Connecting to %s'% self.options['url'])
        if self.sessionThread.is_alive() == False:
            self.__autoUpdateSessionToken()
        self.connection.connect()

    def subscribe(self, optionOrStreamId, callback):
        if type(optionOrStreamId) == str:
            options = {'stream': optionOrStreamId}
        elif type(optionOrStreamId) == dict:
            options = optionOrStreamId
        else:
            raise Exception('subscribe: options must be an object. Given :%s'%(type(optionOrStreamId)))

        options = {**self.options, **options}

        if options.get('stream',None) == None:
            raise Exception('subscribe : Invalid arguments: options[\'steram\'] is not given')
        
        sub = Subscription(options['stream'],options.get('partition',0),options.get('apiKey',options.get('auth',{}).get('apiKey',None)),callback,options)
       
        def gap_handler(from_,to_):
            if not sub.resending:
                self.__requestResend(sub,{'resend_from':from_,'resend_to':to_})
        sub.on('gap', gap_handler)

        def done_handler():
            logger.debug('done event for sub %s '%(sub.id))
            self.unsubscribe(sub)
        sub.on('done', done_handler)

        self.__addSubscription(sub)

        if  self.connection.state == Connection.State.CONNECTED:
            self.__resendAndSubscribe(sub)
        elif self.options.get('autoConnect',False):
            try:
                self.connect()
            except:
                pass
        return sub

    def unsubscribe(self,sub):
        if sub == None or not isinstance(sub,Subscription):
            raise Exception('unsubscribe: please give a subscription object as an argument')
        
        if self.subsByStream.get(sub.streamId,None) != None and len(self.subsByStream.get(sub.streamId,None)) == 1 and self.isConnected() and sub.getState() == Subscription.State.subscribed:
            sub.setState(Subscription.State.unsubscribing)
            self.__requestUnsubscribe(sub.streamId)

        elif sub.getState()!= Subscription.State.unsubscribing and sub.getState() != Subscription.State.unsubscribed:
            self.__removeSubscription(sub)
            sub.setState(Subscription.State.unsubscribed)
            self.__checkAutoDisconnect()

    def unsubscribeAll(self,streamId):
        if not streamId:
            raise Exception('unsubscribeAll: a stream id is required')
        elif type(streamId) != str:
            raise Exception('unsubscribe: stream id must be a string')

        if self.subsByStream.get(streamId,None):
            subs = copy.copy(self.subsByStream[streamId])
            for sub in subs:
                self.unsubscribe(sub)

    def isConnected(self):
        return self.connection.state == Connection.State.CONNECTED

    def reconnected(self):
        self.connect()

    def pause(self):
        self.connection.disconnect()

    def disconnect(self):
        self.subsByStream = defaultdict(list)
        self.subById = {}
        self.connection.disconnect()
        self.sessionThread.cancel()

    def __checkAutoDisconnect(self):
        if self.options.get('autoDisconnect',False) and len(self.subsByStream.keys()) == 0:
            logger.debug('Disconnecting due to no longer being subscribed to any streams')
            self.disconnect()

    def __resendAndSubscribe(self,sub):
        if sub.getState() != Subscription.State.subscribing and not sub.resending:
            sub.setState(Subscription.State.subscribing)

            def subscribed():
                if sub.hasResendOptions():
                    self.__requestResend(sub)
            sub.once('subscribed',subscribed)

            self.__requestSubscribe(sub)

    
    def __requestSubscribe(self,sub):

        subs = self.subsByStream.get(sub.streamId,[])

        subscribedSubs = []
        for s in subs:
            if s.getState() == Subscription.State.subscribed:
                subscribedSubs.append(s)


        if len(subscribedSubs) == 0:
            request = SubscribeRequest(sub.streamId,0,sub.apiKey,self.sessionToken)
            logger.debug('_requestSubscribing client :%s'%(request))
            self.connection.send(request)
        elif len(subscribedSubs) > 0:
            logger.debug('__requestSubscribe: another subscription for same stream : %s, insta-subscribing'%(sub.streamId))
            sub.setState(Subscription.State.subscribed)

    def __requestUnsubscribe(self,streamId,partition=0,apiKey=None):
        logger.debug('Client unsubscribing stream %s'%(streamId))
        self.connection.send(UnsubscribeRequest(streamId,partition,apiKey if apiKey else self.options['apiKey'],self.sessionToken))

    def __requestResend(self,sub,resendOptions = None):
        sub.setResending(True)
        request = ResendRequest(sub.streamId,sub.streamPartition,sub.id,resendOptions if resendOptions != None else sub.getEffectiveResendOptions(),sub.apiKey,self.sessionToken)
        logger.debug('__requestResend :%s'%(request))
        self.connection.send(request)

    def __requestPublish(self,streamId,data,apiKey):
        request = PublishRequest(streamId,apiKey,self.sessionToken,data)
        logger.debug('__requestPublish :%s'%(request))
        self.connection.send(request)

    def handleError(self,msg):
        logger.debug(msg)
        self.emit('error',msg)

    def createStream(self, streamname, streamdes=None):
        stream = creating(streamname,streamdes,self.sessionToken)
        return stream

    def getStreamByName(self,streamname):
        return getting_by_name(streamname,self.sessionToken)

    def getStreamById(self,streamId):
        return getting_by_id(streamId,self.sessionToken)

    def getOrCreateStream(self,streamname):
        stream = self.getStreamByName(streamname)
        if len(stream) == 0 or stream == None:
            return self.createStream(streamname)
        else:
            return stream

