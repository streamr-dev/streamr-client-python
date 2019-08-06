"""
constant
"""


class Addrs:
    """
    Server address
    """

    WS_ADDR = 'wss://www.streamr.com/api/v1/ws'
    REST_ADDR = 'https://www.streamr.com/api/v1'


class RestfullConstant:
    """
    Restful constants
    """
    GET_SESSION_ADDR = Addrs.REST_ADDR + '/login/apikey'
    GET_SESSION_PARAS = {"Content-Type": "application/json"}
    GET_SESSION_BODY = '{"apiKey": "%s"}'

    CREATE_ADDR = Addrs.REST_ADDR + '/streams'
    CREATE_BODY = '{"name": %s, "description": %s, "config": {"fields":[]}}'
    GET_BY_ID_ADDR = CREATE_ADDR + '/'
    GET_BY_NAME_ADDR = CREATE_ADDR + '?name='

    PARAS_KEY = "Authorization"
    PARAS_VALUE = "Bearer %s"

    SESSION_TOKEN = 'token'


class StreamMessageConstant:
    """
    store the key of dict of StreamMessage
    """

    STREAM_ID = 'streamId'
    STREAM_PARTITION = 'streamPartition'
    TIMESTAMP = 'timestamp'
    TTL = 'ttl'
    OFFSET = 'offset'
    PREVIOUS_OFFSET = 'previousOffset'
    CONTENT_TYPE = 'contentType'
    CONTENT = 'content'
    SIGNATURE_TYPE = 'signatureType'
    PUBLISHER_ADDRESS = 'publisherAddress'
    SIGNATURE = 'signature'
    BYE = '_bye'


class StreamAndPartitionConstant:
    """
    store the key of dict of StreamAndPartitionConstant
    """

    STREAM_ID = 'stream'
    STREAM_PARTITION = 'partition'


class ResendResponsePayloadConstant:
    """
    store the key of dict of ResendResponsePayloadConstant
    """
    STREAM_ID = 'stream'
    STREAM_PARTITION = 'partition'
    SUB_ID = 'sub'


class ErrorPayloadConstant:
    """
    store the key of dict of ErrorPayloadConstant
    """
    ERROR = 'error'


class RequestConstant:
    """
    store the key of dict of RequestConstant
    """
    TYPE = 'type'
    STREAM_ID = 'stream'
    API_KEY = 'authKey'
    SESSION_TOKEN = 'sessionToken'
    SERIALIZED_CONTENT = 'msg'
    TIMESTAMP = 'ts'
    PARTITION_KEY = 'pkey'
    PUBLISHER_ADDRESS = 'addr'
    SIGNATURE_TYPE = 'sigtype'
    SIGNATURE = 'sig'
    STREAM_PARTITION = 'partition'
    SUB_ID = 'sub'


class AuthConstant:
    """
    store the key of dict of Auth
    """
    API_KEY = 'apiKey'
    PRIVATE_KEY = 'privateKey'
    PROVIDER = 'provider'
    USER_NAME = 'username'
    PASSWORD = 'password'
    SESSION_TOKEN = 'sessionToken'


class OptionConstant:
    """
    store the key of dict of Options
    """
    API_KEY = 'apiKey'
    URL = 'url'
    REST_URL = 'restUrl'
    AUTO_CONNECT = 'autoConnect'
    AUTO_DISCONNECT = 'autoDisconnect'
    SESSION_REFRESH_INTERVAL = 'interval'
    AUTH = 'auth'
    AUTH_KEY = 'authKey'

    STREAM_ID = 'stream'
    STREAM_PARTITION = 'partition'
    SUB_ID = 'sub'
    RESEND_LABEL = 'resend_'
    RESEND_LAST = 'resend_last'
    RESEND_FROM = 'resend_from'
    RESEND_TO = 'resend_to'
    RESEND_FROM_TIME = 'resend_from_time'
    RESEND_ALL = 'resend_all'


class EventConstant:
    """
    store the event name for client, connection and subscription
    """

    CONNECTING = 'CONNECTING'
    CONNECTED = 'CONNECTED'
    DISCONNECTING = 'DISCONNECTING'
    DISCONNECTED = 'DISCONNECTED'
    SUBSCRIBING = 'SUBSCRIBING'
    SUBSCRIBED = 'SUBSCRIBED'
    UNSUBSCRIBING = 'UNSUBSCRIBING'
    UNSUBSCRIBED = 'UNSUBSCRIBED'
    ERROR = 'ERROR'
    RESENDING = 'RESENDING'
    RESENT = 'RESENT'
    NO_RESEND = 'NO_RESEND'
    GAP = 'GAP'
    DONE = 'DONE'

