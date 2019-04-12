"""
provide parser to parse json object and time object
"""


import json
import logging


__all__ = ['jparser', 'tparser']


def jparser(msg):
    """
    json parser
    :param msg: str file list dict
    :return: list or dict
    """
    if isinstance(msg, str):
        return json.loads(msg)
    elif hasattr(msg, 'read'):
        return json.load(msg)
    elif isinstance(msg, (list, dict)):
        return msg
    else:
        raise ValueError('Unsupported object for jparser. given : %s' % (type(msg)))


def tparser(msg):
    """
    timestmap parser
    :param msg: float int str
    :return: float or int
    """
    if isinstance(msg, (float, int)):
        return msg

    if isinstance(msg, str):
        try:
            timestamp = int(msg)
            return timestamp
        except ValueError:
            try:
                timestamp = float(msg)
                return timestamp
            except ValueError:
                logging.warning('Unknown time format %s' % msg)
