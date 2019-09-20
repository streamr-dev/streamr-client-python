"""
Event class
"""


from collections import defaultdict
import logging


class Event:
    """
    Event class
    """

    def __init__(self):
        self.eventList = defaultdict(list)
        self.eventListOnce = defaultdict(list)

    def on(self, eventname, callback):
        """
        listen to an event
        :param eventname: str
        :param callback: callback func
        :return: None
        """
        if callback in self.eventList[eventname]:
            logging.warning('Event %s has been added' % callback)
        else:
            self.eventList[eventname].append(callback)

    def off(self, eventname, callback):
        """
        stop listen to an event
        :param eventname:  str
        :param callback:  callback func
        :return:  None
        """
        if callback in self.eventList[eventname]:
            self.eventList[eventname].remove(callback)
        else:
            logging.warning('Event: %s has no callback function: %s' %
                            (eventname, callback))

    def emit(self, eventname, *args):
        """
        emit a received event
        :param eventname: str
        :param args: args of callback func
        :return:
        """
        if eventname in self.eventList.keys():
            list(map(lambda x: x(*args), self.eventList[eventname]))
        if eventname in self.eventListOnce.keys():
            list(map(lambda x: x(*args), self.eventListOnce[eventname]))
            self.eventListOnce[eventname].clear()
        logging.info('received eventname %s ' % eventname)

    def once(self, eventname, callback):
        """
        listen to a event and only for once
        :param eventname: str
        :param callback: callback function
        :return: None
        """
        if callback in self.eventListOnce[eventname]:
            logging.warning(
                'Event %s has been added in list_once' % callback)
        else:
            self.eventListOnce[eventname].append(callback)
