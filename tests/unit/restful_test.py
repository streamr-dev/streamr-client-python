"""
test restful
"""


from streamr.rest.session import get_session_token_by_api_key
from streamr.rest.stream import creating, getting_by_id, getting_by_name

from tests.config import get_api_key

api = get_api_key()
assert api is not None

session_token = get_session_token_by_api_key(api)
assert session_token is not None

create_result = creating('lx', 'this is for testing ', session_token)
assert create_result is not None

name_result = getting_by_name('lx', session_token)
assert name_result is not None

id_result = getting_by_id(create_result['id'], session_token)
assert id_result is not None

print('restfull test passed')
