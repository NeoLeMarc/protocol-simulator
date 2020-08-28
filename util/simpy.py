#!/usr/bin/env python3
# Some tools to help test simpy based applications


class ExtractorEvent(object):
    """Simules an simpy event and stores the value supplied to succeed into _item.
       Can be used to extract values from stores"""
    
    def __init__(self):
        self._ok = True
        self._value = ""
        self._item = None

    def succeed(self, item):
        print("Succeed called")
        self._item = item
