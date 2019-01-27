from streamr.protocol.utils.parser import JParser
from streamr.protocol.utils.meta import RequestMeta
from streamr.protocol.utils.parser import Tparser
from functools import reduce
import json


class Request(metaclass=RequestMeta):

    def __init__(self, requestType, streamId=None, apiKey=None, sessionToken=None):

        self.requestType = requestType
        self.streamId = streamId
        self.apiKey = apiKey
        self.sessionToken = sessionToken
    
    def toObject(self):
        return {'type':self.requestType,'stream':self.streamId,'authKey':self.apiKey,'sessionToken':self.sessionToken}

    def serialize(self):
        return json.dumps(self.toObject())

    @classmethod
    def checkVersion(cls,msg):
        version = msg.get('version',None) or 0
        if (version != 0):
            raise Exception('UnsupportedVersion : %s. Supported version : [0]' % version )

    @classmethod
    def deserialize(cls, msg):
        msg = JParser(msg)
        cls.checkVersion(msg)
        args = cls.messageClassByMessageType[msg['type']].getConstructorArguments(msg)
        return cls.messageClassByMessageType[msg['type']](*args)

    def __eq__(self,another):
        if type(self) == type(another) :
            return self.requestType == another.requestType and self.streamId == another.streamId and self.apiKey == another.apiKey and self.sessionToken == another.sessionToken 
        else:
            return False

class PublishRequest(Request):

    TYPE = 'publish'

    def __init__(self,streamId,apiKey,sessionToken,content,timestamp=None,partitionKey=None,publisherAddress=None,signatureType=None,signature=None):
        super().__init__(self.TYPE,streamId,apiKey,sessionToken)
        if content is None:
            raise Exception('No content given')

        self.content = content
        
        self.timestamp = timestamp

        self.partitionKey = partitionKey
        self.publisherAddress = publisherAddress
        self.signatureType = signatureType
        self.signature = signature

    def getTimestampAsNumber(self):
        if self.timestamp:
            return Tparser(self.timestamp)
        return None

    def getSerializedContent(self):
        if type(self.content) == str:
            return self.content
        elif type(self.content) in [list,dict]:
            return json.dumps(self.content)

        raise Exception('Stream payload can only be objects')

    def toObject(self):
        return {**super().toObject() , **{'msg':self.getSerializedContent(),
                                'ts':self.getTimestampAsNumber(),
                                'pkey':self.partitionKey,
                                'addr':self.publisherAddress,
                                'sigtype':self.signatureType,
                                'sig':self.signature}}
    
    @classmethod
    def getConstructorArguments(self,msg):
        return [msg.get('stream',None),
                msg.get('authKey',None),
                msg.get('sessionToken',None),
                msg.get('msg',None),
                msg.get('ts',None),
                msg.get('pkey',None),
                msg.get('addr',None),
                msg.get('sigtype',None),
                msg.get('sig',None)]

    def __eq__(self,another):
        if type(self) == type(another):
            return self.content == another.content and self.timestamp == another.timestamp and \
            self.partitionKey == another.partitionKey and self.publisherAddress == another.publisherAddress and \
            self.signatureType == another.signatureType and self.signature == another.signature and super().__eq__(another)
        else:
            return False


class ResendRequest(Request):
    TYPE = 'resend'

    def __init__(self,streamId,streamPartition = 0,subId = None,resendOptions=None,apiKey=None,sessionToken=None):
        super().__init__(self.TYPE, streamId, apiKey,sessionToken)

        if not resendOptions['resend_all'] and not resendOptions['resend_from_time']:
            raise Exception('Invalid resend options')
        
        if not subId:
            raise Exception('Subscription ID not given')
        
        self.streamPartition = streamPartition
        self.subId = subId
        self.resendOptions = resendOptions

    def toObject(self):
        return {**super().toObject(),  **{'partition':self.streamPartition,
                                'sub':self.subId} ,**self.resendOptions}

    @classmethod
    def getConstructorArguments(cls,msg):
        resendOptions = {}
        for key in msg.keys():
            if str.startswith(key,'resend_'):
                resendOptions[key] = msg[key]

        return [msg.get('stream',None),
                msg.get('partition',None),
                msg.get('sub',None),
                resendOptions,
                msg.get('authKey',None),
                msg.get('sessionToken',None)]

    def __eq__(self, another):
        if type(self) == type(another):
            return self.streamPartition == another.streamPartition and \
            self.subId == another.subId and self.resendOptions == another.resendOptions and \
            super().__eq__(another)
        else:
            return False


class SubscribeRequest(Request):
    TYPE = 'subscribe'
    def __init__(self,streamId,streamPartition=0,apiKey = None,sessionToken=None):
        super().__init__(self.TYPE,streamId,apiKey,sessionToken)

        self.streamPartition = streamPartition

    def toObject(self):
        return {**super().toObject(),'partition':self.streamPartition}

    @classmethod
    def getConstructorArguments(self,msg):
        return [msg.get('stream',None),
                msg.get('partition',None),
                msg.get('authKey',None),
                msg.get('sessionToken',None)]

    def __eq__(self, another):
        if type(self) == type(another):
            return self.streamPartition == another.streamPartition and super().__eq__(another)
        else:
            return False

class UnsubscribeRequest(Request):
    TYPE = 'unsubscribe'

    def __init__(self,streamId,streamPartition=0,apiKey = None,sessionToken = None):
        super().__init__(self.TYPE,streamId,apiKey,sessionToken)
        
        self.streamPartition = streamPartition

    def toObject(self):
        return {**super().toObject(),'partition':self.streamPartition}

    @classmethod
    def getConstructorArguments(cls,msg):
        return [msg.get('stream',None),
                msg.get('partition',None)]

    def __eq__(self, another):
        if type(self) == type(another):
            return self.streamPartition == another.streamPartition and super().__eq__(another)
        else:
            return False
