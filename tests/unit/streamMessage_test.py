from streamr.protocol.payloads import StreamMessage, InvalidJsonError, UnsupportedVersionError
import json
import time

if __name__=='__main__':


    # streammessage version 28
    arr = [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
    941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}']

    msg = StreamMessage.deserialize(arr)

    assert isinstance(msg,StreamMessage)
    assert msg.streamId == 'TsvTbqshTsuLg_HyUjxigA'
    assert msg.streamPartition == 0
    assert msg.timestamp == 1529549961116
    assert msg.ttl == 0
    assert msg.offset == 941516902
    assert msg.previousOffset == 941499898
    assert msg.contentType == StreamMessage.CONTENT_TYPE.JSON
    assert msg.content == '{"valid": "json"}'

    # streammessage version 28  content invalid
    arr = [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
            941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"invalid\njson"}']

    try:
        StreamMessage.deserialize(arr)
    except Exception as e:
        assert isinstance(e, InvalidJsonError)
        assert e.streamId == 'TsvTbqshTsuLg_HyUjxigA'
        assert e.jsonString == '{"invalid\njson"}'
        assert isinstance(e.streamMessage,StreamMessage)
    

    # streammessage version 28  serialize
    arr = [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
       941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}']
    serialized = StreamMessage(
        'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0, 941516902, 941499898,StreamMessage.CONTENT_TYPE.JSON,'{"valid": "json"}').serialize(28)
    assert serialized == json.dumps(arr)


    # streammessage version 29  deserialize
    arr = [29, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                  941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}', 1, 'address', 'signature']
    
    result = StreamMessage.deserialize(arr)

    assert isinstance(result,StreamMessage)

    assert result.streamId == 'TsvTbqshTsuLg_HyUjxigA'
    assert result.streamPartition == 0
    assert result.timestamp == 1529549961116
    assert result.ttl == 0
    assert result.offset == 941516902
    assert result.previousOffset == 941499898
    assert result.contentType == StreamMessage.CONTENT_TYPE.JSON
    assert result.content == '{"valid": "json"}'
    assert result.signatureType == 1
    assert result.publisherAddress == 'address'
    assert result.signature == 'signature'

    # streammessage version 29  serialize
    arr = [29, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
       941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}', 1, 'address', 'signature']
    serialized = StreamMessage('TsvTbqshTsuLg_HyUjxigA', 0,
                  1529549961116, 0, 941516902, 941499898,StreamMessage.CONTENT_TYPE.JSON,'{"valid": "json"}',1,'address','signature').serialize(29)
    assert serialized == json.dumps(arr)

    # unsupported version deserialize
    arr = [123]

    try:
        StreamMessage.deserialize(arr)
    except Exception as e:
        assert isinstance(e,UnsupportedVersionError)
        assert e.version == 123

    # unsupported version serialize
    try:
        StreamMessage('TsvTbqshTsuLg_HyUjxigA', 0,
              1529549961116, 0, 941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid":"json"}', 1, 'address', 'signature').serialize(123)
    except Exception as e:
        assert isinstance(e,UnsupportedVersionError)
        assert e.version == 123

    # getParsedContent
    content = {'foo':'bar'}
    msg = StreamMessage('streamId',0,time.time(),0,1,None,StreamMessage.CONTENT_TYPE.JSON,json.dumps(content))
    
    dic = msg.getParsedContent() 
    assert dic == content


    content = {'foo': 'bar'}
    msg = StreamMessage('streamId',0,time.time(),0,1,None,StreamMessage.CONTENT_TYPE.JSON,content)
    dic = msg.getParsedContent()
    assert dic == content

    # to object parse is true
    obj = [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                    941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, {
                        'valid': 'json',
                    }]

    msg = StreamMessage(
                'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}',
            )
    lizt = msg.toObject(28,True)

    assert obj == lizt


    # to object  compact is false
    obj = {
        'streamId': 'TsvTbqshTsuLg_HyUjxigA',
        'streamPartition': 0,
        'timestamp': 1529549961116,
        'ttl': 0,
        'offset': 941516902,
        'previousOffset': 941499898,
        'contentType': StreamMessage.CONTENT_TYPE.JSON,
        'content': '{"valid": "json"}'
    }

    msg = StreamMessage(
                'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}'
            )

    dic = msg.toObject(28, False , False)

    assert dic == obj


    # to object  compact is false parse is True
    obj = {
        'streamId': 'TsvTbqshTsuLg_HyUjxigA',
        'streamPartition': 0,
        'timestamp': 1529549961116,
        'ttl': 0,
        'offset': 941516902,
        'previousOffset': 941499898,
        'contentType': StreamMessage.CONTENT_TYPE.JSON,
        'content': {
            'valid': 'json'
        }}

    msg = StreamMessage(
                'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}'
    )

    dic = msg.toObject(28, True, False)
    assert dic == obj

