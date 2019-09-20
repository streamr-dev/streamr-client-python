"""
an example of connect and publish
"""


from streamr import Client, Option
import logging
import time

logging.basicConfig(level=logging.INFO)
my_option = Option.get_default_option()
my_option.auto_disconnect = False
my_option.auto_connect = False
my_option.api_key = 'your-api-key'
my_client = Client(my_option)

# To connect to the server, you can use connect function
# This function returns immediately and you can query the state by is_connected function
my_client.connect()

while my_client.is_connected() is False:
    pass


# create or get a stream by its name
stream = my_client.get_or_create_stream('stream_test')

# To publish data to a stream, you should subscribe to the stream at first
# you can use subscribe function
# Two parameters are needed:
# 1. 'stream_id':
# 		you can find the stream_id on the website: streamr.com
# 		You can also get the stream_id by using 'get_or_create_stream',
# 		'get_stream_by_name', 'get_stream_by_id' functions
# 2. callback function:
# 		callback function will run when client received data from server to this stream
#
# the return is a subscription object

# get stream_id

stream_id = stream[0]['id']


def callback(parsed_msg, msg):
    """
    callback function print the received data from server
    :param parsed_msg: dict 
    :param msg: msg object
    :return:
    """
    logging.info('received message. Content : %s, stream_id :%s stream_partition : %s'
                 % (parsed_msg, msg.stream_id, msg.stream_partition))


subscrip = my_client.subscribe(stream_id, callback)


# To publish data you can use publish function
# Two parameters are needed:
# 1. 'stream_id' as a string object or a object containing the 'stream_id' attribute
# 2. data as a dictionary object

data = {"name": 'google', "age": 19}

my_client.publish(subscrip, data)

# wait 5 seconds and disconnect
time.sleep(5)
my_client.disconnect()
