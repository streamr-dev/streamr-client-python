"""
test subscription
"""


from streamr.client.subscription import Subscription
from streamr.protocol.payload import StreamMessage
import time

from streamr.util.option import Option
from streamr.util.constant import EventConstant
from streamr.protocol.errors.error import InvalidJsonError


stream_id = 'stream_id'
stream_partition = 0


def create_msg(offset=1, previous_offset=None, content=None):
    """
    create a message
    :param offset:
    :param previous_offset:
    :param content:
    :return:
    """
    if content is None:
        content = dict()
    return StreamMessage(stream_id, stream_partition, time.time(),
                         0, offset, previous_offset,
                         StreamMessage.ContentType.JSON, content)


def test_handle_message():

    msg = create_msg()

    def callback(content, receive_msg):
        """
        callback function when subscription received data from server
        :param content:
        :param receive_msg:
        :return:
        """
        assert isinstance(content, type(msg.get_parsed_content()))
        assert content == msg.get_parsed_content()
        assert receive_msg is msg

    sub = Subscription(stream_id, stream_partition, 'api_key', callback)
    sub.handle_message(msg)


def test_handle_messages():

    msgs = list(map(lambda x: create_msg(
        x, None if x is None else x-1), [1, 2, 3, 4, 5]))

    received = []

    def callback(content, received_msg):
        """
        callback function when subscription received data from server
        :param content:
        :param received_msg:
        :return:
        """
        received.append(received_msg)
        if len(received) == 5:
            for i in range(5):
                assert msgs[i] is received[i]
                assert content == {}

    sub = Subscription(stream_id, stream_partition, 'api_key', callback)

    for m in msgs:
        sub.handle_message(m)


def test_handle_message_when_resending_and_is_resend_false():

    msg = create_msg()

    count = 0

    def callback(_, __):
        """
        callback function to test the number of received msg
        :param _:
        :param __:
        :return:
        """
        nonlocal count
        count += 1

    sub = Subscription(stream_id, stream_partition, 'api_key', callback)

    sub.set_resending(True)
    sub.handle_message(msg)
    assert count == 0
    assert(len(sub.queue) == 1)


def test_handle_message_when_resending_and_is_resend_true():

    msg = create_msg()

    count = 0

    def callback(_, __):
        """
        callback function to test the number of received msg
        :param _:
        :param __:
        :return:
        """
        nonlocal count
        count += 1

    sub = Subscription(stream_id, stream_partition, 'api_key', callback)
    sub.set_resending(True)
    sub.handle_message(msg, True)
    assert(count == 1)


def test_duplicate_handling():

    msg = create_msg()

    count = 0

    def callback(_, __):
        """
        callback function to test whether has received msg
        :param _:
        :param __:
        :return:
        """
        nonlocal count
        count += 1

    sub = Subscription(stream_id, stream_partition, 'api_key', callback)
    sub.handle_message(msg)
    sub.handle_message(msg)
    assert count == 1


def test_duplicate_handling_when_resending():
    msg = create_msg()

    count = 0

    def callback(_, __):
        """
        callback function to test whether has received msg
        :param _:
        :param __:
        :return:
        """
        nonlocal count
        count += 1

    sub = Subscription(stream_id, stream_partition, 'api_key', callback)

    sub.handle_message(msg)
    sub.handle_message(msg, True)
    assert count == 1


def test_gap():

    count = 0

    def callback(_, __):
        """
        callback function to test whether has received msg
        :param _:
        :param __:
        :return:
        """
        nonlocal count
        count += 1

    msg1 = create_msg()
    msg4 = create_msg(4, 3)

    sub = Subscription(stream_id, stream_partition, 'api_key', callback)

    def on_gap(from_, to_):
        """
        test whether gap is detected
        :param from_:
        :param to_:
        :return:
        """
        assert from_ == 2
        assert to_ == 3
        print('gap detected')

    sub.on('gap', on_gap)

    sub.handle_message(msg1)
    sub.handle_message(msg4)
    assert count == 1


def test_no_gap():

    count = 0

    def callback(_, __):
        """
        callback function to test whether has received msg
        :param _:
        :param __:
        :return:
        """
        nonlocal count
        count += 1

    msg1 = create_msg()
    msg2 = create_msg(2, 1)
    sub = Subscription(stream_id, stream_partition, 'api_key', callback)

    def on_gap(_, __):
        """
        gap should not be detected
        :param _:
        :param __:
        :return:
        """
        raise Exception('GapDetectError')

    sub.on('gap', on_gap)

    sub.handle_message(msg1)
    sub.handle_message(msg2)
    assert count == 2


def test_bye():
    count = 0

    def callback(_, __):
        """
        callback function to test whether has received msg
        :param _:
        :param __:
        :return:
        """
        nonlocal count
        count += 1

    bye_msg = create_msg(1, None, {'BYE_KEY': True})
    sub = Subscription(
        bye_msg.stream_id, bye_msg.stream_partition, 'api_key', callback)

    def test():
        assert count == 1

    sub.on('done', test)

    sub.handle_message(bye_msg)


def test_handle_error():

    err = Exception('Test error')

    def callback(_, __):
        """
        should not be called
        :param _:
        :param __:
        :return:
        """
        raise Exception('handleErrorFailed')

    sub = Subscription(stream_id, stream_partition, 'api_key', callback)

    def test(msg):
        assert isinstance(msg, Exception)
        assert str(msg) == 'Test error'

    sub.on('error', test)
    sub.handle_error(err)


def test_invalid_json_error():

    def callback(_, __):
        """
        empty callback func
        :param _:
        :param __:
        :return:
        """
        print('done')

    sub = Subscription(stream_id, stream_partition, 'api_key', callback)

    def on_gap(_, __):
        """
        should not detect a gap
        :param _:
        :param __:
        :return:
        """
        raise Exception('GapDetectError')

    sub.on('gap', on_gap)

    msg1 = create_msg()
    msg3 = create_msg(3, 2)

    sub.handle_message(msg1)

    error = InvalidJsonError(stream_id, 'invalid json',
                             'test error msg', create_msg(2, 1))

    sub.handle_error(error)
    sub.handle_message(msg3)


def test_get_original_resend_option():

    def callback(_, __):
        """
        empty callback func
        :param _:
        :param __:
        :return:
        """
        print('done')

    sub = Subscription(stream_id, stream_partition,
                       'api_key', callback, Option(resend_all=True))

    assert sub.get_effective_resend_option().resend_all is True


def test_get_resend_option_after_received_data1():
    msg = create_msg()

    def callback(_, __):
        """
        empty callback func
        :param _:
        :param __:
        :return:
        """
        print('done')
    sub = Subscription(stream_id, stream_partition,
                       'api_key', callback, Option(resend_all=True))
    sub.handle_message(msg)

    dic = sub.get_effective_resend_option()

    assert dic.resend_from == 2


def test_get_resend_option_after_received_data2():

    def callback(_, __):
        """
        empty callback func
        :param _:
        :param __:
        :return:
        """
        print('done')
    sub = Subscription(stream_id, stream_partition,
                       'api_key', callback, Option(resend_from=1))
    sub.handle_message(create_msg(10))
    dic = sub.get_effective_resend_option()
    assert dic.resend_from == 11


def test_get_resend_option_after_received_data3():

    def callback(_, __):
        """
        empty callback func
        :param _:
        :param __:
        :return:
        """
        print('done')
    sub = Subscription(stream_id, stream_partition,
                       'api_key', callback, Option(resend_from_time=time.time()))
    sub.handle_message(create_msg(10))
    dic = sub.get_effective_resend_option()
    assert dic.resend_from == 11


def test_get_resend_option_after_received_data4():
    msg = create_msg()

    def callback(_, __):
        """
        empty callback func
        :param _:
        :param __:
        :return:
        """
        print('done')
    sub = Subscription(stream_id, stream_partition,
                       'api_key', callback, Option(resend_last=10))
    sub.handle_message(msg)
    dic = sub.get_effective_resend_option()
    assert dic.resend_last == 10


def test_update_state():

    sub = Subscription(stream_id, stream_partition,
                       'api_key', lambda: print('done'))
    sub.set_state(EventConstant.SUBSCRIBED)
    assert sub.get_state() == EventConstant.SUBSCRIBED


def test_emit_event():
    count = 0

    sub = Subscription(stream_id, stream_partition,
                       'api_key', lambda: print('done'))

    def test():
        nonlocal count
        count += 1
    sub.on(EventConstant.SUBSCRIBED, test)

    sub.set_state(EventConstant.SUBSCRIBED)

    assert count == 1


def test_event_handling_resent():

    msg = create_msg()

    count = 0

    def test(_, __):
        nonlocal count
        count += 1

    sub = Subscription(stream_id, stream_partition, 'apiKay', test)

    sub.set_resending(True)
    sub.handle_message(msg)
    assert count == 0

    sub.emit(EventConstant.RESENT)
    assert count == 1


def test_event_handling_no_resent():
    count = 0

    msg = create_msg()

    def test(_, __):
        nonlocal count
        count += 1

    sub = Subscription(stream_id, stream_partition, 'api_key', test)

    sub.set_resending(True)
    sub.handle_message(msg)
    assert count == 0
    sub.emit(EventConstant.NO_RESEND)
    assert count == 1


