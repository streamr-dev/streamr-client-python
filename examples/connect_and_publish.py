from streamr.client.client import Client


myOption = {'apiKey': 'your-api-key'}
myClient = Client(myOption)


# To connect to the server, you can use connect method
# This method returns immediately  and you can query the state by isConnected funciton
myClient.connect()

# wait for connected
while(myClient.isConnected() == False):
    pass


# To publish data to a stream, you should subscribe to the stream at first
# you can use subscribe method
# the return is a subscription object
# you can also add callback funciton and it will run when client received data from this stream

def callback(parsedMsg, msg):
    print('received message. Content : ', parsedMsg, ' streamId : ',
          msg.streamId, 'streamParition : ', msg.streamPartition)


subscrip = myClient.subscribe('streamId', callback)


# To publish data you can use publish function
# Two parameter are needed: 1.streamId or objects containning the 'streamid' attribute 2. data whihc is a dictionary object

data = {"name": 'google', "age": 19}
myClient.publish(subscrip, data)
