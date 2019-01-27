from collections import defaultdict
import logging


class Event():

    def __init__(self):
        self.event_list = defaultdict(list)
        self.event_list_once = defaultdict(list)

    def on(self,event_name,callback):
        if callback in self.event_list[event_name]:
            logging.warning('Event %s has been added'%(callback))
        else:
            self.event_list[event_name].append(callback)

    def off(self,event_name,callback):
        if callback in self.event_list[event_name]:
            self.event_list[event_name].remove(callback)
        else:
            logging.warning('Event: %s has no callback function: %s'&(event_name,callback))

    def emit(self,event_name,*args):
        if event_name in self.event_list.keys():
            list(map(lambda x :x(*args),self.event_list[event_name]))
        if event_name in self.event_list_once.keys():
            list(map(lambda x: x(*args), self.event_list_once[event_name]))
            self.event_list_once[event_name].clear()
    
    def once(self,event_name,callback):
        if callback in self.event_list_once[event_name]:
            logging.warning('Event %s has been added in list_once'%(callback))
        else:
            self.event_list_once[event_name].append(callback)
