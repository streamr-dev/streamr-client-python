


## Streamr-Client-Python

By using this client, you can easily interact with the [Streamr](http://www.streamr.com) API from python environments. You can, for example, subscribe to real-time data in Streams, produce new data to Streams, and create new Streams.

This library is work-in-progress


### Installation

1. download

2. `cd streamr-client-python`

3. `pip install .` 

### Tests
You can test the integration functions using the following steps.

`client_test.py`:  you can run this file to test.

```
$ cd streamr-client-python
$ python -m tests.integration.client_test
```

### Usage

Here are some quick examples.

#### Importing streamr-client-python module

```
from streamr import Client, Option
```

#### Creating a StreamrClient instance with given option

```
option = Option.get_default_option()
option.api_key = 'your-api-key'
option.auto_connect = False
option.auto_disconnect = False
client = Client(option)
```
#### getting or creating a stream by name

```
stream = client.get_or_create_stream('stream-test')

```

#### getting the stream id

```
stream_id = stream[0]['id']

```

#### getting stream by stream_name or stream_id

```
stream = client.get_stream_by_name('stream-test')
```
```
stream = client.get_stream_by_id(stream_id)
``` 

#### checking the state of connection

```
print(client.connection.state)
```

#### connect to server

```
client.connect()
while(not client.is_connected()):
    pass
```

#### Subscribing to stream
```
def callback(msg,_):
	print('message received . The Cotent is : %s'%(msg))

subscription = client.subscribe(stream_id, callback)
```


#### publishing data to stream

```
data = [{"name":'google',"age":19},{"name":"yahoo","age":11},{"name":"facebook","age":13},{"name":"twitter","age":1}]
for d in data:
    client.publish(subscription, d)
```

#### disconnect from server

```
client.disconnect()
```