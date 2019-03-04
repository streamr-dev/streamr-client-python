
import threading
import logging
import time

from streamr.client.client import Client
from tests.config import getAPIKey

def testClient():
    logging.basicConfig(level=logging.ERROR, 
                        format='%(relativeCreated)6d %(threadName)s %(levelname)s :%(message)s')



    cli = Client({'apiKey': getAPIKey(), 'autoConnect': False,
                  'autoDisconnect': False})

    while(cli.sessionToken is None):
        pass

    sessionToken = cli.sessionToken

    assert sessionToken is not None

    stream = cli.getOrCreateStream('stream-test')
    assert(isinstance(stream, list))
    for s in stream:
        assert s['name'] == 'stream-test'

    streamByName = cli.getStreamByName('stream-test')
    assert(isinstance(streamByName, list))
    for s in stream:
        assert(s['name'] == 'stream-test')

    streamId = stream[0]['id']
    streamById = cli.getStreamById(streamId)
    assert(isinstance(streamById, dict))
    assert(streamById['id'] == streamId)

    cli.connect()

    while(not cli.isConnected()):
        pass

    msg = [{"name": 'google', "age": 19}, {"name": "facebook", "age": 11},
           {"name": "yahoo", "age": 13}, {"name": "twitter", "age": 1}]

    def callback(parsedMsg, msgObject):
        assert msgObject.streamId == streamId
        assert (parsedMsg) in msg

    subscription = cli.subscribe(streamId, callback)

    for m in msg:
        cli.publish(subscription, m)

    time.sleep(30)
    cli.disconnect()

    print('tests passed')


if __name__ == "__main__":
    testClient()
