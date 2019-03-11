"""
an example of connect and publish
"""


from streamr.client.client import Client
from streamr.util.option import Option

my_option = Option.get_default_option()
my_option.auto_disconnect = False
my_option.auto_connect = False

my_client = Client(my_option)

# To connect to the server, you can use connect function
# This function returns immediately and you can query the state by is_connected funciton
my_client.connect()

# wait for connected
while my_client.is_connected() is False:
    pass


# To publish data to a stream, you should subscribe to the stream at first
# you can use subscribe function
# Two parameters are needed:
# 1. 'stream-sub_id':
# 		you can find the stream-sub_id on the website: streamr.com
# 		You can also get the stream-sub_id by using 'get_or_create_stream',
# 		'get_stream_by_name', 'get_tream_by_id' functions
# 2. callback function:
# 		callback function will run when client received data from server to this stream
#
# the return is a subscription object

def callback(parsed_msg, msg):
    """
    callback function print the received data from server
    :param parsed_msg:
    :param msg:
    :return:
    """
    print('received message. Content : ', parsed_msg, ' stream_id : ',
          msg.stream_id, 'stream_parition : ', msg.stream_partition)


subscrip = my_client.subscribe('stream_id', callback)


# To publish data you can use publish function
# Two parameters are needed:
# 1. 'stream-sub_id' as a string object or a object containning the 'stream_id' attribute
# 2. data as a dictionary object

data = {"name": 'google', "age": 19}

my_client.publish(subscrip, data)

# Note: you should replace 'stream_id' with your own stream_id
