"""
an example of create Client
"""

from streamr import Client, Option


# To create a client object, you can use this function.

my_option = Option.get_default_option()

# my_option.api_key = 'your-api-Key'
my_option.api_key = '27ogvnHOQhGFQGETwjf1dAWFd2wXHbTlKCj_uEUTESXw'
my_client = Client(my_option)

# you can also change the option value like this
my_option.auto_connect = False
my_option.auto_disconnect = False

# Notes:
# 1. You should replace 'your-api-key' with your own api key
# 2. The client will autoconnect to server by default.

my_client.disconnect()

