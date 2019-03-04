from collections import defaultdict
import logging


class Event:

    def __init__(self):
        self.eventList = defaultdict(list)
        self.eventListOnce = defaultdict(list)

    def on(self, eventname, callback):
        if callback in self.eventList[eventname]:
            logging.warning('Event %s has been added' % (callback))
        else:
            self.eventList[eventname].append(callback)

    def off(self, eventname, callback):
        if callback in self.eventList[eventname]:
            self.eventList[eventname].remove(callback)
        else:
            logging.warning('Event: %s has no callback function: %s' %
                            (eventname, callback))

    def emit(self, eventname, *args):
        if eventname in self.eventList.keys():
            list(map(lambda x: x(*args), self.eventList[eventname]))
        if eventname in self.eventListOnce.keys():
            list(map(lambda x: x(*args), self.eventListOnce[eventname]))
            self.eventListOnce[eventname].clear()

    def once(self, eventname, callback):
        if callback in self.eventListOnce[eventname]:
            logging.warning(
                'Event %s has been added in list_once' % (callback))
        else:
            self.eventListOnce[eventname].append(callback)
