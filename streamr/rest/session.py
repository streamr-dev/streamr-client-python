#!/usr/bin/python3
import requests
import json
import logging
import threading as thd
import time


__all__ = ['getSeToken_APIKey']


logger = logging.getLogger(__name__)

def getSeToken_APIKey(apikey):
    """
       func:
	   Get a session token using the API key. NOTE: Auto-update every two hours.
       paras:
	   apikey: the streamr API key (in string type)
       return:
	   SessionToken in string type.(if success)
	   None: (if failure)
    """
    url = 'https://www.streamr.com/api/v1/login/apikey'
    paras = {"Content-Type": "application/json"}
    body = ('{"apiKey": "%s"}' % apikey)
    logger.debug("The creating request url is: %s, the paras are: %s, the body is: %s" % (
        url, paras, body))
    try:
        req = requests.post(url, headers=paras, data=body)
        if req.status_code == 200 or req.status_code == 201:
            logger.info("Get a Session Token successfully. %s" % req.json())
            SessionToken = req.json()['token']
            
            return str(SessionToken)
        else:
            logger.error("Fail to get a session token. The Status code: %s" %
                      req.status_code)
            return None
    except:
        import traceback
        logger.error("Fail to get a Session Token.")
        traceback.print_exc()
        return None

