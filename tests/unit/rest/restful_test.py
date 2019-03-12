"""
test restful
"""


from streamr.rest.session import get_session_token_by_api_key
from streamr.rest.stream import creating, getting_by_id, getting_by_name
from tests.config import get_api_key


def test_get_token():
    api = get_api_key()
    st = get_session_token_by_api_key(api)
    assert isinstance(st, str)
    return st


def test_creating_stream(name_, st):
    create_result = creating(name_, 'this is for testing ', st)
    assert isinstance(create_result, dict)
    assert create_result['name'] == name_
    return create_result['id']


def test_get_stream_by_name(name_, st):
    name_result_1 = getting_by_name(name_, st)
    assert isinstance(name_result_1, list)
    assert len(name_result_1) == 1

    creating(name_, 'this is also for testing', st)
    name_result_2 = getting_by_name(name_, st)
    assert isinstance(name_result_2, list)
    assert len(name_result_2) == 2

    name_result_3 = getting_by_name('invalid', st)
    assert name_result_3 is None


def test_get_stream_by_id(s_id, st):
    id_result_1 = getting_by_id(s_id, st)
    assert isinstance(id_result_1, dict)
    assert id_result_1['id'] == s_id

    id_result_2 = getting_by_id('not_existed', st)
    assert id_result_2 is None


if __name__ == '__main__':
    name = 'a1'
    session_token = test_get_token()
    stream_id = test_creating_stream(name, session_token)
    test_get_stream_by_name(name, session_token)
    test_get_stream_by_id(stream_id, session_token)
    print('restful test passed')

