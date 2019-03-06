from streamr.client.client import Client


# To create a client object, you can use this function.

myOption = {'apiKey': 'your-api-key'}
myClient = Client(myOption)

# you can also configure your client by adding key-value in option dictionary
myOption = {'apiKey': 'your-api-key',
            'autoConnect': False, 'autoDisconnect': False}
