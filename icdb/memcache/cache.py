# -------------------------------#
# Written by icoz, 2013          #
# email: icoz.vt at gmail.com    #
# License: GPL v3                #
# -------------------------------#

# if __name__ == "__main__" and __package__ is None:
#     __package__ = "icdb.memcache"

from icdb.storage.storage import Storage
from datetime import datetime, timedelta
import os


class Cache(object):

    """Cache is simple mem-storage for key-value, based on dict()"""

    def __init__(self, fname=None):
        '''
        if fname is passed, then try to load from file
        '''
        super(Cache, self).__init__()
        self.data = dict()
        if fname is not None:
            if os.path.exists(fname):
                self.load(fname)

    def get(self, key):
        if type(key) is not str:
            key = str(key)
        try:
            val = self.data[key]
        except KeyError:
            return None
        return val

    def set(self, key, value):
        if type(key) is not str:
            key = str(key)
        self.data[key] = value

    def delete(self, key):
        if type(key) is not str:
            key = str(key)
        try:
            del self.data[key]
        except KeyError:
            pass

    def save(self, fname=None):
        if fname is not None:
            with Storage(fname) as s:
                for k in self.data:
                    s.set_unsafe(k, self.data[k])

    def load(self, fname=None):
        if fname is not None:
            with Storage(fname) as s:
                self.data = s.get_dict()
