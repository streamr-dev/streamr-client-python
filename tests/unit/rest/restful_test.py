"""
test restful
"""

from streamr.rest.session import get_session_token_by_api_key
from streamr.rest.stream import creating, getting_by_id, getting_by_name


import random


def test_rest():

    api = '27ogvnHOQhGFQGETwjf1dAWFd2wXHbTlKCj_uEUTESXw'
    session_token = get_session_token_by_api_key(api)
    assert isinstance(session_token, str)

    name = ''.join(random.sample('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890', 20))

    create_result = creating(name, 'this is for testing ', session_token)
    assert isinstance(create_result, dict)
    assert create_result['name'] == name
    stream_id = create_result['id']

    nameresult_1 = getting_by_name(name, session_token)
    assert isinstance(nameresult_1, list)
    assert len(nameresult_1) == 1

    creating(name, 'this is also for testing', session_token)
    nameresult_2 = getting_by_name(name, session_token)
    assert isinstance(nameresult_2, list)
    assert len(nameresult_2) == 2

    nameresult_3 = getting_by_name('invalid', session_token)
    assert nameresult_3 is None

    id_result_1 = getting_by_id(stream_id, session_token)
    assert isinstance(id_result_1, dict)
    assert id_result_1['id'] == stream_id

    id_result_2 = getting_by_id('not_existed', session_token)
    assert id_result_2 is None
