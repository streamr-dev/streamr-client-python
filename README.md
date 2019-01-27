


## Streamr-Client-Python

By using this client, you can easily interact with the [Streamr](http://www.streamr.com) API from python environments. You can, for example, subscribe to real-time data in Streams, produce new data to Streams, and create new Streams.

This library is work-in-progress


. Note: this library only supports Python3. ### Installation
This module is test on Python 3.6+

1. download

2. `cd streamr-client-python`

3. `pip3 install .`

### Tests
You can test the integration functions using the following files.

`client_test.ipynb`: If you installed the jupyter library, you can run this file to test.

`client_test.py`: If you don't have the jupyter library, you can run this file to test.

### Usage

Here are some quick examples.

#### Importing streamr-client-python module

```python
from streamr.client.client import Client
```

#### Creating a StreamrClient instance with given options

```python
options = {'apiKey':'your-apiKey','autoConnect':False,'autoDisconnect':False}

client = Client(options)
```
#### getting or creating a stream by name

```python
stream = client.getOrCreateStream('stream-test')
```

#### gettting the stream id

```python
streamId = stream['id']
```

#### getting stream by streamName or streamId

```python
stream = client.getStreamByName('stream-test')

stream = client.getStreamById(streamId)
``` 

#### checking the state of connection

```python
print(client.connection.state)
```

#### connect to server

```python
client.connect()
while(client.connection.state != client.connection.State.Connected):
	pass
```

#### Subscribing to stream
```python
def callback(msg,msgObj):
	print('message received . The Cotent is : %s'%(msg))

subscription = client.subscribe({'stream':streamId}, callback)
```


#### publishing data to stream

```python

data = [{"name":'google',"age":19},{"name":"yahoo","age":11},{"name":"facebook","age":13},{"name":"twitter","age":1}]
for d in data:
    client.publish(subscription, d)
```

#### disconnect from server

```python
client.disconnect()
```

