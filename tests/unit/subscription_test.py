from streamr.client.subscription import Subscription
from streamr.protocol.payloads import StreamMessage
import time
from streamr.client.event import Event
from streamr.protocol.errors.error import InvalidJsonError

def createMsg(offset=1,previousOffset = None,content = {}):
    return StreamMessage('streamId',0,time.time(),0,offset,previousOffset,StreamMessage.CONTENT_TYPE.JSON,content)

msg = createMsg()

def callback1(content,receiveMsg):
    assert type(content) == type(msg.getParsedContent())
    assert content == msg.getParsedContent()
    assert receiveMsg is msg


sub = Subscription(msg.streamId, msg.streamPartition, 'apiKey', callback1)
sub.handleMessage(msg)


msgs = list(map(lambda x :createMsg(x, None if x == None else x-1),[1,2,3,4,5]))

received = []

def callback2(content,receivedMsg):
    received.append(receivedMsg)
    print(content,len(received))
    if len(received) == 5:
        for i in range(5):
            assert msgs[i] is received[i]
            print('pass equal')



sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',callback2)

for m in msgs:
    sub.handleMessage(m)


count = 0
def handler1(parseContent,msg):
    global count
    count += 1

sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',handler1)

sub.setResending(True)
sub.handleMessage(msg)
assert count == 0
assert(len(sub.queue) == 1)




sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',handler1)
sub.handleMessage(msg,True)
assert(count == 1)



def handler2(a,b):
    print('done')


sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',handler2)
sub.handleMessage(msg)
sub.handleMessage(msg)


sub = Subscription(msg.streamId,msg.streamPartition,'apikey',handler2)

sub.handleMessage(msg)
sub.handleMessage(msg,True)

def handler3(a,b):
    pass



msg1 = msg
msg4 = createMsg(4,3)


sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',handler3)

def onGap(from_,to_):
    assert from_ == 2
    assert to_ == 3
    print('gap detected')

sub.on('gap',onGap)

sub.handleMessage(msg1)
sub.handleMessage(msg4)


msg1 = msg
msg2 = createMsg(2,1)
sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',handler3)
def onGap2(from_, to_):
    print('should not see this line')
sub.on('gap',onGap2)

sub.handleMessage(msg1)
sub.handleMessage(msg2)

count = 0
def handler4(x,y):
    global count
    count += 1

byeMsg = createMsg(1, None, {'BYE_KEY': True})
sub = Subscription(byeMsg.streamId,byeMsg.streamPartition,'apiKey',handler4)

def lis():
    assert count == 1
sub.on('done', lis)

sub.handleMessage(byeMsg)



err = Exception('Test error')

def handler5(x,y):
    raise Exception('this handler should not be called')

sub = Subscription(msg.streamId,msg.streamPartition,'apikey',handler5)
def li(msg):
    print(isinstance(msg, Exception),msg)

sub.on('error',li)
sub.handleError(err)


def hand(x,y):
    pass

sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',hand)

sub.on('gap',handler5)

msg1 = msg
msg3 = createMsg(3,2)

sub.handleMessage(msg1)

error = InvalidJsonError(msg.streamId,'invalid json','test error msg',createMsg(2,1))

sub.handleError(error)
sub.handleMessage(msg3)

def hand(x,y):
    print('here')

sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',hand,{'resend_all':True})

assert sub.getEffectiveResendOptions().get('resend_all',False) == True

sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',hand,{'resend_all':True})
sub.handleMessage(msg)

dic = sub.getEffectiveResendOptions()

assert dic == {'resend_from':2}

sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',hand,{'resend_from':1})
sub.handleMessage(createMsg(10))
dic = sub.getEffectiveResendOptions()
assert dic == {'resend_from':11}


sub = Subscription(msg.streamId,msg.streamPartition,'apikey',hand,{'resend_from_time':time.time()})
sub.handleMessage(createMsg(10))
dic = sub.getEffectiveResendOptions()
assert dic == {'resend_from':11}

sub = Subscription(msg.streamId,msg.streamPartition,'apikey',hand,{'resend_last':10})
sub.handleMessage(msg)
dic = sub.getEffectiveResendOptions()
assert dic == {'resend_last':10}


sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',hand)
sub.setState(Subscription.State.subscribed)
assert sub.getState() == Subscription.State.subscribed

def emp():
    pass

sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',hand)

sub.on(Subscription.State.subscribed,emp)

sub.setState(Subscription.State.subscribed)


count = 0
def emp(x,y):
    global count
    count += 1

sub = Subscription(msg.streamId,msg.streamPartition,'apiKay',emp)

sub.setResending(True)
sub.handleMessage(msg)
assert count == 0

sub.emit('resent')
assert count == 1

count = 0
def emp(x,y):
    global count
    count += 1
sub = Subscription(msg.streamId,msg.streamPartition,'apiKey',emp)

sub.setResending(True)
sub.handleMessage(msg)
assert count == 0
sub.emit('no_resend')
assert count == 1