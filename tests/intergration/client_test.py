#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import threading
import logging
from streamr.client.client import Client
from streamr.rest import session

import json
import time


# In[2]:


logging.basicConfig(level=logging.ERROR,format='%(relativeCreated)6d %(threadName)s %(levelname)s :%(message)s')


# In[3]:


apiKey = '27ogvnHOQhGFQGETwjf1dAWFd2wXHbTlKCj_uEUTESXw'


# In[4]:


cli = Client({'apiKey':apiKey,'autoConnect':False,'autoDisconnect':False,'sessionTokenRefreshInterval':10})


# In[5]:


print(cli.sessionToken)
time.sleep(15)
print(cli.sessionToken)


# In[6]:


stream = cli.getOrCreateStream('stream-test')


# In[7]:


stream


# In[8]:


streamByName = cli.getStreamByName('stream-test')


# In[9]:


streamByName


# In[10]:


streamId = streamByName[0]['id']


# In[11]:


streamId


# In[12]:


cli.getStreamById(streamId)


# In[13]:


cli.connection.state


# In[14]:


cli.connect()
while(cli.connection.state != cli.connection.State.CONNECTED):
    pass


# In[15]:


def callback(parsedMsg,msg):
    print('received message. Content : ',parsedMsg,' streamId : ',msg.streamId,'streamParition : ',msg.streamPartition)


# In[17]:


subscription = cli.subscribe(streamId,callback)


# In[20]:


msg = [{"name":'google',"age":19},{"name":"facebook","age":11},{"name":"yahoo","age":13},{"name":"twitter","age":1}]
for m in msg:
    cli.publish(subscription,m)


# In[21]:


print(cli.sessionToken)
time.sleep(15)
print(cli.sessionToken)


# In[22]:


cli.disconnect()


# In[23]:


print(cli.sessionToken)
time.sleep(12)
print(cli.sessionToken)

