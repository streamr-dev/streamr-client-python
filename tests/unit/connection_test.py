from streamr.protocol.payloads import *
from streamr.client.connection import Connection
from streamr.client.event import Event
from streamr.protocol.request import *
from streamr.protocol.response import *

import time



conn = Connection({'url': 'ws://echo.websocket.org/'})

assert conn.state == Connection.State.DISCONNECTED

conn.connect()
assert conn.state == Connection.State.CONNECTING

while(conn.state != Connection.State.CONNECTED):
    time.sleep(1)
assert conn.state == Connection.State.CONNECTED

conn.disconnect()
assert conn.state == Connection.State.DISCONNECTING
while(conn.state != Connection.State.DISCONNECTED):
    time.sleep(1)
assert conn.state == Connection.State.DISCONNECTED



msg = [0, 0, None, [28, 'TsvTbqshTsuLg_HyUjxigA', 0, 1529549961116, 0,
                    941516902, 941499898, StreamMessage.CONTENT_TYPE.JSON, '{"valid": "json"}']]
result = Response.deserialize(json.dumps(msg))

conn.connect()
while(conn.state != Connection.State.CONNECTED):
    time.sleep(1)

def broad(msg):
    print(type(msg))
    print(msg.payload.streamId)


def e(msg):
    print(msg)

conn.on('BroadcastMessage',broad)
conn.on('error',e)
conn.send(result)


count = 0
while(True):
    count += 1
    time.sleep(3)
    print('main thread is still alive')

    if count %3 == 0:
        conn.send(result)