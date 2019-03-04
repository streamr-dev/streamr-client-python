from streamr.client.subscription import Subscription
from streamr.protocol.payloads import StreamMessage
import time
from streamr.client.event import Event
from streamr.protocol.errors.error import InvalidJsonError


streamId = 'streamId'
streamPartition = 0


def createMsg(offset=1, previousOffset=None, content={}):
    return StreamMessage(streamId, streamPartition, time.time(), 0, offset, previousOffset, StreamMessage.CONTENT_TYPE.JSON, content)


def testHandleMessage():

    msg = createMsg()

    def callback(content, receiveMsg):
        assert isinstance(content, type(msg.getParsedContent()))
        assert content == msg.getParsedContent()
        assert receiveMsg is msg

    sub = Subscription(streamId, streamPartition, 'apiKey', callback)
    sub.handleMessage(msg)


def testHandleMessages():

    msgs = list(map(lambda x: createMsg(
        x, None if x == None else x-1), [1, 2, 3, 4, 5]))

    received = []

    def callback(content, receivedMsg):
        received.append(receivedMsg)
        if len(received) == 5:
            for i in range(5):
                assert msgs[i] is received[i]

    sub = Subscription(streamId, streamPartition, 'apiKey', callback)

    for m in msgs:
        sub.handleMessage(m)


def testHandleMessageWhenResendingWithIsResendFalse():

    msg = createMsg()

    count = 0

    def callback(parseContent, msg):
        nonlocal count
        count += 1

    sub = Subscription(streamId, streamPartition, 'apiKey', callback)

    sub.setResending(True)
    sub.handleMessage(msg)
    assert count == 0
    assert(len(sub.queue) == 1)


def testHandleMessageWhenResendingWithIsResendTrue():

    msg = createMsg()

    count = 0

    def callback(parseContent, msg):
        nonlocal count
        count += 1

    sub = Subscription(streamId, streamPartition, 'apiKey', callback)
    sub.setResending(True)
    sub.handleMessage(msg, True)
    assert(count == 1)


def testDuplicatehandling():

    msg = createMsg()

    def callback(parseContent, msg):
        print('done')

    sub = Subscription(streamId, streamPartition, 'apiKey', callback)
    sub.handleMessage(msg)
    sub.handleMessage(msg)


def testDuplicatehandlingWithResendingFlg():
    msg = createMsg()

    def callback(parseContent, msg):
        print('done')

    sub = Subscription(streamId, streamPartition, 'apikey', callback)

    sub.handleMessage(msg)
    sub.handleMessage(msg, True)


def testGap():

    def callback(a, b):
        print('done')

    msg1 = createMsg()
    msg4 = createMsg(4, 3)

    sub = Subscription(streamId, streamPartition, 'apiKey', callback)

    def onGap(from_, to_):
        assert from_ == 2
        assert to_ == 3
        print('gap detected')

    sub.on('gap', onGap)

    sub.handleMessage(msg1)
    sub.handleMessage(msg4)


def testNoGap():

    def callback(a, b):
        print('done')

    msg1 = createMsg()
    msg2 = createMsg(2, 1)
    sub = Subscription(streamId, streamPartition, 'apiKey', callback)

    def onGap(from_, to_):
        raise Exception('GapDetectError')

    sub.on('gap', onGap)

    sub.handleMessage(msg1)
    sub.handleMessage(msg2)


def testBye():
    count = 0

    def callback(x, y):
        nonlocal count
        count += 1

    byeMsg = createMsg(1, None, {'BYE_KEY': True})
    sub = Subscription(
        byeMsg.streamId, byeMsg.streamPartition, 'apiKey', callback)

    def test():
        assert count == 1

    sub.on('done', test)

    sub.handleMessage(byeMsg)


def testHandleError():

    err = Exception('Test error')

    def callback(x, y):
        raise Exception('handleErrorFailed')

    sub = Subscription(streamId, streamPartition, 'apikey', callback)

    def test(msg):
        assert isinstance(msg, Exception)
        assert str(msg) == 'Test error'

    sub.on('error', test)
    sub.handleError(err)


def testInvalidJsonError():

    def callback(x, y):
        print('done')

    sub = Subscription(streamId, streamPartition, 'apiKey', callback)

    def onGap(from_, to_):
        raise Exception('GapDetectError')

    sub.on('gap', onGap)

    msg1 = createMsg()
    msg3 = createMsg(3, 2)

    sub.handleMessage(msg1)

    error = InvalidJsonError(streamId, 'invalid json',
                             'test error msg', createMsg(2, 1))

    sub.handleError(error)
    sub.handleMessage(msg3)


def testGetOriginalResendOptions():

    def callback(x, y):
        print('done')

    sub = Subscription(streamId, streamPartition,
                       'apiKey', callback, {'resend_all': True})

    assert sub.getEffectiveResendOptions().get('resend_all', False) == True


def testGetResendOptionAfterReceivedData1():
    msg = createMsg()

    def callback(x, y):
        print('done')
    sub = Subscription(streamId, streamPartition,
                       'apiKey', callback, {'resend_all': True})
    sub.handleMessage(msg)

    dic = sub.getEffectiveResendOptions()

    assert dic == {'resend_from': 2}


def testGetResendOptionAfterReceivedData2():

    def callback(x, y):
        print('done')
    sub = Subscription(streamId, streamPartition,
                       'apiKey', callback, {'resend_from': 1})
    sub.handleMessage(createMsg(10))
    dic = sub.getEffectiveResendOptions()
    assert dic == {'resend_from': 11}


def testGetResendOptionAfterReceivedData3():

    def callback(x, y):
        print('done')
    sub = Subscription(streamId, streamPartition,
                       'apikey', callback, {'resend_from_time': time.time()})
    sub.handleMessage(createMsg(10))
    dic = sub.getEffectiveResendOptions()
    assert dic == {'resend_from': 11}


def testGetResendOptionAfterReceivedData4():
    msg = createMsg()

    def callback(x, y):
        print('done')
    sub = Subscription(streamId, streamPartition,
                       'apikey', callback, {'resend_last': 10})
    sub.handleMessage(msg)
    dic = sub.getEffectiveResendOptions()
    assert dic == {'resend_last': 10}


def testUpdateState():

    sub = Subscription(streamId, streamPartition,
                       'apiKey', lambda: print('done'))
    sub.setState(Subscription.State.SUBSCRIBED)
    assert sub.getState() == Subscription.State.SUBSCRIBED


def testEmitEvent():
    count = 0

    sub = Subscription(streamId, streamPartition,
                       'apiKey', lambda: print('done'))

    def test():
        nonlocal count
        count += 1
    sub.on(Subscription.State.SUBSCRIBED, test)

    sub.setState(Subscription.State.SUBSCRIBED)

    assert count == 1


def testEventHandlingResent():

    msg = createMsg()

    count = 0

    def test(x, y):
        nonlocal count
        count += 1

    sub = Subscription(streamId, streamPartition, 'apiKay', test)

    sub.setResending(True)
    sub.handleMessage(msg)
    assert count == 0

    sub.emit('resent')
    assert count == 1


def testEventHandlingNoResent():
    count = 0

    msg = createMsg()

    def test(x, y):
        nonlocal count
        count += 1

    sub = Subscription(streamId, streamPartition, 'apiKey', test)

    sub.setResending(True)
    sub.handleMessage(msg)
    assert count == 0
    sub.emit('no_resend')
    assert count == 1


if __name__ == '__main__':
    testBye()
    testDuplicatehandling()
    testDuplicatehandlingWithResendingFlg()
    testEmitEvent()
    testEventHandlingNoResent()
    testEventHandlingResent()
    testGap()
    testGetOriginalResendOptions()
    testGetResendOptionAfterReceivedData1()
    testGetResendOptionAfterReceivedData2()
    testGetResendOptionAfterReceivedData3()
    testGetResendOptionAfterReceivedData4()
    testHandleError()
    testHandleMessage()
    testHandleMessages()
    testHandleMessageWhenResendingWithIsResendFalse()
    testHandleMessageWhenResendingWithIsResendTrue()
    testInvalidJsonError()
    testNoGap()
    testUpdateState()
