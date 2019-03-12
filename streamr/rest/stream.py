"""
provide functions to create stream and get stream
"""


import requests
import logging
from streamr.util.constant import RestfullConstant

__all__ = ['creating', 'getting_by_name', 'getting_by_id']

logger = logging.getLogger(__name__)


def creating(stream_name, stream_des, session_token):
    """
    func:
        Create a Stream using the stream name and description.
    paras:
        stream_name:  the stream name
        stream_des:   the stream description.
    return:
        status_code: True means creating a stream successfully, while None means creating it unsuccessfully.
    """

    url = RestfullConstant.CREATE_ADDR
    paras = {RestfullConstant.PARAS_KEY: RestfullConstant.PARAS_VALUE % session_token}
    body = RestfullConstant.CREATE_BODY % (stream_name, stream_des)
    logger.info("The creating request url is: %s, the paras are: %s, the body is: %s" % (
        url, paras, body))
    try:
        req = requests.post(url, headers=paras, data=body, verify=True)
        if req.status_code == 200 or req.status_code == 201:
            logger.info("Create a Stream successfully.")
            return req.json()
        else:
            logger.error("Fail to Create a Stream. The Status code: %s" %
                         req.status_code)
            return None
    except requests.RequestException:
        import traceback
        logger.error("Fail to Create a Stream.")
        traceback.print_exc()
        return None


def getting_by_id(stream_id, session_token):
    """
    func:
        Getting a Stream by the stream sub_id
    paras:
        stream_id: The stream sub_id
    return:
        req.json(): The request responses in json type.
    """
    url = RestfullConstant.GET_BY_ID_ADDR + stream_id
    paras = {RestfullConstant.PARAS_KEY: RestfullConstant.PARAS_VALUE % session_token}
    logger.info(
        "The getting_by_id request url is: %s, the paras are: %s" % (url, paras))
    try:
        req = requests.get(url, headers=paras, verify=True)
        if req.status_code == 200 or req.status_code == 201:
            logger.info(
                "Getting a Stream using its ID successfully, and the stream is: %s" % req.json())
            return req.json()
        else:
            logger.error(
                "Fail to Get a Stream using its ID : %s. The Status code: %s" % (stream_id, req.status_code))
            return None
    except requests.RequestException:
        import traceback
        logger.error("Fail to Get a Stream using its ID : %s " % stream_id)
        traceback.print_exc()
        return None


def getting_by_name(stream_name, session_token):
    """
    func:
        Getting a Stream by the stream name.
        NOTE: "Case Insensitive"
    paras:
        stream_name: The stream name
    return:
        req.text: The request responses in list type.
    """
    url = RestfullConstant.GET_BY_NAME_ADDR + stream_name
    paras = {RestfullConstant.PARAS_KEY: RestfullConstant.PARAS_VALUE % session_token}
    logger.info(
        "The getting_by_name request url is: %s, the paras are: %s" % (url, paras))
    try:
        req = requests.get(url, headers=paras, verify=True)
        if req.status_code == 200 or req.status_code == 201:
            if len(req.json()) == 0:
                logger.error(
                     "Fail to Get a Stream using its name: %s" % stream_name)
                return None
            else:
                logger.info(
                    "Getting a Stream using its name successfully, and the stream is: %s" % req.text)
                return req.json()
        else:
            logger.error(
                "Fail to Get a Stream using its name. The Status code: %s" % req.status_code)
            return None
    except requests.RequestException:
        import traceback
        logger.error("Fail to Get a Stream using its name: %s " % stream_name)
        traceback.print_exc()
        return None
