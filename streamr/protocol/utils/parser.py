import json
import time


def JParser(msg):
    if type(msg) == str:
        return json.loads(msg)
    elif hasattr(msg,'read'):
        return json.load(msg)
    elif type(msg) in [list,dict]:
        return msg
    else:
        raise Exception('Unknow type %s' % (type(msg)))


def Tparser(msg):
    if type(msg) == float or type(msg) == int:
        return msg

    if type(msg) == str:
        try:
            timestamp = int(msg)
            return timestamp
        except:
            # logging.warn('Unknow time format %s'%msg)
            pass

        try:
            timestamp = float(msg)
            return timestamp
        except:
            # logging.warn('Unknow time format %s'%msg)
            pass

        try:
            timestamp = time.mktime(time.strptime(msg, '%Y-%m-%d %H:%M%S'))
            return timestamp
        except:
            # logging.warn('Unknow time format %s'%msg)
            pass

        try:
            timestamp = time.mktime(time.strptime(msg, '%M %m-%d %H:%M%S'))
            return timestamp
        except:
            # logging.warn('Unknow time format %s'%msg)
            pass

