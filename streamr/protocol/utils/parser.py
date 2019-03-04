import json
import time
import logging

from streamr.protocol.errors.error import ParameterError

__all__ = ['JParser', 'TParser']


def JParser(msg):
    if isinstance(msg, str):
        return json.loads(msg)
    elif hasattr(msg, 'read'):
        return json.load(msg)
    elif isinstance(msg, (list, dict)):
        return msg
    else:
        raise ParameterError('Unsupported msg type: %s' % (type(msg)))


def TParser(msg):
    if isinstance(msg, (float, int)):
        return msg

    if isinstance(msg, str):
        try:
            timestamp = int(msg)
            return timestamp
        except:
            try:
                timestamp = float(msg)
                return timestamp
            except:
                logging.warn('Unknow time format %s' % msg)
