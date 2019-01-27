from streamr.protocol.errors.error import InvalidJsonError , UnsupportedVersionError
from streamr.protocol.utils.parser import JParser
import json


class StreamMessage:

    class CONTENT_TYPE:
        JSON = 27

    def __init__(self,streamId,streamPartition,timestamp,ttl,offset,previousOffset,contentType,content,signatureType=None,publisherAddress=None,signature=None):
        self.streamId = streamId
        self.streamPartition = streamPartition
        self.timestamp = timestamp
        self.ttl = ttl
        self.offset = offset
        self.previousOffset = previousOffset
        self.contentType = contentType
        self.content = content
        self.signatureType = signatureType
        self.publisherAddress = publisherAddress
        self.signature = signature
        self.parsedContent = None

    def getParsedContent(self):
        if self.parsedContent != None:
            return self.parsedContent
        elif self.contentType == StreamMessage.CONTENT_TYPE.JSON and type(self.content) in [list,dict]:
            self.parsedContent = self.content
        elif self.contentType == StreamMessage.CONTENT_TYPE.JSON and type(self.content) == str:
            try:
                self.parsedContent = json.loads(self.content)
            except Exception as e:
                raise   InvalidJsonError(self.streamId,self.content,e,self)
        else:
            raise Exception('Unsupported content type: %s' % self.contentType)
        return self.parsedContent
    
    def getSerializedContent(self):
        if type(self.content) == str:
            return self.content
        elif type(self.contentType == StreamMessage.CONTENT_TYPE.JSON) and type(self.content) != str:
            return json.dumps(self.content)
        elif type(self.contentType == StreamMessage.CONTENT_TYPE.JSON):
            raise Exception('Stream payloads can only be objects')
        else:
            raise Exception('Unsupported content type: %s' %self.contentType)

    def toObject(self,version=28,parsedContent = False,compact = True):
        if version == 28 or version == 29:
            if compact:
                arr = [version, self.streamId, self.streamPartition, self.timestamp, self.ttl, self.offset, self.previousOffset,
                       self.contentType, self.getParsedContent() if parsedContent else self.getSerializedContent()]
                if version == 29:
                    arr.append(self.signatureType)
                    arr.append(self.publisherAddress)
                    arr.append(self.signature)
            else:
                arr = {'streamId': self.streamId,
                    'streamPartition': self.streamPartition,
                    'timestamp': self.timestamp,
                    'ttl': self.ttl,
                    'offset': self.offset,
                    'previousOffset': self.previousOffset,
                    'contentType': self.contentType,
                    'content': self.getParsedContent() if parsedContent else self.getSerializedContent(),
                    }
                if version == 29:
                    arr['signatureType'] = self.signatureType
                    arr['publisherAddress'] = self.publisherAddress
                    arr['signarture'] = self.signature
            return arr
        else:
            raise UnsupportedVersionError(
                version, 'Supported version:[ 28, 29 ]')


    def serialize(self,version=28):
        return json.dumps(self.toObject(version))
    
    @classmethod
    def deserialize(cls,msg,parseContent=True):
        
        msg = JParser(msg)

        '''
        Version 28: [version, streamId, streamPartition, timestamp, ttl, offset, previousOffset, contentType, content]
        Version 29: [version, streamId, streamPartition, timestamp, ttl, offset, previousOffset, contentType, content,
                        * signatureType, address, signature]
        '''
        
        if msg[0] == 28 or msg[0] == 29:
            result = cls(*msg[1:])

            if parseContent:
                result.getParsedContent()
            
            return result
        else:
            raise UnsupportedVersionError(msg[0],'Supported version: [ 28, 29]')

    def isByeMessage(self):
        bye = self.getParsedContent().get('BYE_KEY',False)
        return bye

    def __eq__(self,another):
        if type(self) == type(another):
            return self.streamId == another.streamId and self.streamPartition == another.streamPartition and \
            self.timestamp == another.timestamp and self.ttl == another.ttl and \
            self.offset == another.offset and self.previousOffset == another.previousOffset and \
            self.contentType == another.contentType and self.content == another.content and \
            self.signatureType == another.signatureType and self.publisherAddress == another.publisherAddress and \
            self.signature == another.signature and self.parsedContent == another.parsedContent
        else:
            return False


class StreamAndPartition:

    def __init__(self, streamId, streamPartition):

        self.streamId = streamId
        self.streamPartition = streamPartition

    def toObject(self, version=28):

        return {'stream':  self.streamId,
                'partition': self.streamPartition}

    def serialize(self):
        return json.dumps(self.toObject())

    @classmethod
    def objectToConstructorArgs(self, msg):
        return [msg.get('stream', None),
                msg.get('partition', None)]

    @classmethod
    def deserialize(cls, msg):
        msg = JParser(msg)
        return cls(*cls.objectToConstructorArgs(msg))

    def __eq__(self,another):
        if type(self) == type(another):
            return self.streamId == another.streamId and self.streamPartition == another.partition
        else:
            return False

class ResendResponsePayload(StreamAndPartition):

    def __init__(self, streamId, streamPartition, subId):
        super().__init__(streamId, streamPartition)
        self.subId = subId

    def toObject(self, version=28):
        return {**super().toObject(), 'sub': self.subId}

    @classmethod
    def objectToConstructorArgs(cls, msg):
        return [msg.get('stream', None),
                msg.get('partition', None),
                msg.get('sub', None)]

    def __eq__(self,another):
        if type(self) == type(another):
            return self.subId == another.subId and super().__eq__(another)
        else:
            return False

class ErrorPayload:

    def __init__(self, errorString):
        self.error = errorString

    def toObject(self, version=28):
        return {'error': self.error}

    @classmethod
    def deserialize(cls, msg):
        msg = JParser(msg)
        if not msg.get('error', None):
            raise Exception('Invalid error payload received : %s' %
                            (json.dumps(msg)))
        return ErrorPayload(msg['error'])

    def __eq__(self,another):
        if type(self) == type(another):
            return self.error == another.error
        else:
            return False
