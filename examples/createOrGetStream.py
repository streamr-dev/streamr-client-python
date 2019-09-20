"""
an example of create and get stream
"""


from streamr import Client, Option

# To create or get a stream you should create a client object at first
my_option = Option.get_default_option()
my_option.api_key = 'your-api-key'

my_client = Client(my_option)

# To create a new stream, you can use 'create_stream' or 'get_or_create_stream' with a 'name' parameter
# Note that create_stream is running forcefully.
# That means you can create two stream with same name but the stream_ids are different


stream1 = my_client.create_stream('stream-test-1')

stream2 = my_client.get_or_create_stream('stream-test-2')

# To get a stream, you can use 'get_stream_by_name' or get_stream_by_id' method

# get_stream_by_name will return all the streams with steam name
# the return is a list object containing the information of all streams
stream3 = my_client.get_stream_by_name('stream-test-2')

# get stream by id will return the stream with the stream_id
# the return is a dictionary containing the information of the stream
# Before using this methods, you should replace the stream_id with a 32 bytes strings, which
# can be found in the stream page, also can be obtained using the get_stream_by_name method.
stream_id = stream2[0]['id']
stream4 = my_client.get_stream_by_id(stream_id)
