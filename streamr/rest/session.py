"""
provide functions to get session token
"""


import requests
import logging
from streamr.util.constant import RestfullConstant


__all__ = ['get_session_token_by_api_key']


logger = logging.getLogger(__name__)


def get_session_token_by_api_key(api_key):
    """
       func:
           Get a session token using the API key. NOTE: Auto-update every two hours.
       paras:
           api_key: the streamr API key (in string type)
       return:
           session_token in string type.(if success)
           None: (if failure)
    """
    url = RestfullConstant.GET_SESSION_ADDR
    paras = RestfullConstant.GET_SESSION_PARAS
    body = RestfullConstant.GET_SESSION_BODY % api_key
    logger.debug("The creating request url is: %s, the paras are: %s, the body is: %s" % (
        url, paras, body))
    try:
        req = requests.post(url, headers=paras, data=body, verify=True)
        if req.status_code == 200 or req.status_code == 201:
            logger.info("Get a Session Token successfully. %s" % req.json())
            session_token = req.json()[RestfullConstant.SESSION_TOKEN]
            return str(session_token)
        else:
            logger.error("Failed to get a session token. The Status code: %s" %
                         req.status_code)
            return None
    except requests.RequestException:
        import traceback
        logger.error("Failed to get a Session Token.")
        traceback.print_exc()
        return None
