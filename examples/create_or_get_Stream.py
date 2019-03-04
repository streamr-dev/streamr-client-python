from streamr.client.client import Client

# To create or get a stream you should create a client at first
myClient = Client({'apiKey': 'your-api-key'})

# To create a new stream, you can use 'createStream' or 'getOrCreateStream' with a 'name' parameter
# Note that createStream is running forcefully.
# That means you can create two stream with same name but the streamIds are different


#stream = client.createStream('stream-test')

stream = myClient.getOrCreateStream('stream-test')

# To get a stream, you can use 'getStreamByName' or getStreamById' method

# getStreamByName will return all the streams with steam name
# the return is a list object containning the information of all streams
myClient.getStreamByName('stream-test')

# get streamById will return the stream with the streamId
# the return is a dictionary containning the information of the stream
myClient.getStreamById('streamId')
