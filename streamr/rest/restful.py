#!/usr/bin/python3
import requests
import json
import logging


__all__ = ['creating', 'gettingByName', 'gettingById']


logger = logging.getLogger(__name__)


def creating(streamname, streamdes, sessionToken):
    """
    func:
        Create a Stream using the stream name and describtion.
    paras:
        streamname:  the stream name
        streamdes:   the stream describtion.
    return:
        status_code: True means creating a stream successfully, while None means creating it unsuccessfully.
    """
    url = 'https://www.streamr.com/api/v1/streams'
    paras = {"Authorization": "Bearer %s" % sessionToken}
    body = ('{"name": %s, "description": %s, "config": {"fields":[]}}' % (
        streamname, streamdes))
    logger.info("The creating request url is: %s, the paras are: %s, the body is: %s" % (
        url, paras, body))
    try:
        req = requests.post(url, headers=paras, data=body)
        if req.status_code == 200 or req.status_code == 201:
            logger.info("Create a Stream successfully.")
            return req.json()
        else:
            logger.error("Fail to Create a Stream. The Status code: %s" %
                         req.status_code)
            return None
    except:
        import traceback
        logger.error("Fail to Create a Stream.")
        traceback.print_exc()
        return None


def gettingById(streamid, sessionToken):
    """
    func:
        Getting a Stream by the stream id
    paras:
        streamid: The stream id
    return:
        req.json(): The request responses in json type.
    """
    url = 'https://www.streamr.com/api/v1/streams/' + streamid
    paras = {"Authorization": "Bearer %s" % sessionToken}
    logger.info(
        "The getting_by_id request url is: %s, the paras are: %s" % (url, paras))
    try:
        req = requests.get(url, headers=paras)
        if req.status_code == 200 or req.status_code == 201:
            logger.info(
                "Getting a Stream using its ID successfully, and the stream is: %s" % req.json())
            return req.json()
        else:
            logger.error(
                "Fail to Get a Stream using its ID. The Status code: %s" % req.status_code)
            return None
    except:
        import traceback
        logger.error("Fail to Get a Stream using its ID.")
        traceback.print_exc()
        return None


def gettingByName(streamname, sessionToken):
    """
    func:
        Getting a Stream by the stream name.
        NOTE: "Case Insensitive"
    paras:
        streamname: The stream name
    return:
        req.text: The request responses in list type.
    """
    url = 'https://www.streamr.com/api/v1/streams?name=' + streamname
    paras = {"Authorization": "Bearer %s" % sessionToken}
    logger.info(
        "The getting_by_name request url is: %s, the paras are: %s" % (url, paras))
    try:
        req = requests.get(url, headers=paras)
        if req.status_code == 200 or req.status_code == 201:
            logger.info(
                "Getting a Stream using its name successfully, and the stream is: %s" % req.text)
            return req.json()
        else:
            logger.error(
                "Fail to Get a Stream using its name. The Status code: %s" % req.status_code)
            return None
    except:
        import traceback
        logger.error("Fail to Get a Stream using its name.")
        traceback.print_exc()
        return None
